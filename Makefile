docker_build_args = --rm \
       -u `id -u`:`id -g` \
       -v `pwd`:/io \
       -w /io \
       quay.io/pypa/manylinux2014_x86_64:latest
auditwheel_args = --plat manylinux2014_x86_64

build_dir = build
rust_dir  = rust
venv_dir  = .venv
PYTHON    = $(venv_dir)/bin/python3
PIP       = $(venv_dir)/bin/pip

.PHONY: all
all: install

# ---------------------------------------------------------------------------
# Dependency pre-check
# ---------------------------------------------------------------------------
# Prints a table of required / optional host tools with version hints.
# Exits non-zero if any *required* tool is absent.
.PHONY: check/deps
check/deps:
	@missing=0; \
	ok="[  OK  ]"; bad="[MISSING]"; opt="[ opt.  ]"; \
	\
	check_req() { \
	  tool=$$1; hint=$$2; \
	  if command -v "$$tool" >/dev/null 2>&1; then \
	    ver=$$($$tool --version 2>&1 | head -1); \
	    printf "  %s  %-12s  %s\n" "$$ok" "$$tool" "$$ver"; \
	  else \
	    printf "  %s  %-12s  install: %s\n" "$$bad" "$$tool" "$$hint"; \
	    missing=$$((missing+1)); \
	  fi; \
	}; \
	\
	check_opt() { \
	  tool=$$1; hint=$$2; \
	  if command -v "$$tool" >/dev/null 2>&1; then \
	    ver=$$($$tool --version 2>&1 | head -1); \
	    printf "  %s  %-12s  %s\n" "$$ok" "$$tool" "$$ver"; \
	  else \
	    printf "  %s  %-12s  install: %s\n" "$$opt" "$$tool" "$$hint"; \
	  fi; \
	}; \
	\
	check_py_import() { \
	  py=$$1; mod=$$2; hint=$$3; \
	  if $$py -c "import $$mod; print($$mod.__version__)" >/dev/null 2>&1; then \
	    ver=$$($$py -c "import $$mod; print($$mod.__version__)" 2>&1); \
	    printf "  %s  %-12s  %s\n" "$$ok" "$$mod" "$$ver"; \
	  else \
	    printf "  %s  %-12s  install: %s\n" "$$bad" "$$mod" "$$hint"; \
	    missing=$$((missing+1)); \
	  fi; \
	}; \
	\
	echo ""; \
	echo "  ── Build (C++ / cmake) ─────────────────────────────────────────"; \
	check_req cmake   "apt install cmake  |  brew install cmake"; \
	check_req g++     "apt install build-essential  |  brew install gcc"; \
	check_req ninja   "apt install ninja-build  |  brew install ninja"; \
	\
	echo ""; \
	echo "  ── Rust ────────────────────────────────────────────────────────"; \
	check_req cargo   "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"; \
	check_req rustc   "installed alongside cargo via rustup"; \
	\
	echo ""; \
	echo "  ── Python ──────────────────────────────────────────────────────"; \
	check_req python3 "apt install python3  |  brew install python3"; \
	if python3 -m venv --help >/dev/null 2>&1; then \
	  printf "  %s  %-12s\n" "$$ok" "python3-venv"; \
	else \
	  printf "  %s  %-12s  install: apt install python3-venv\n" "$$bad" "python3-venv"; \
	  missing=$$((missing+1)); \
	fi; \
	if [ -x "$(PYTHON)" ]; then \
	  printf "  %s  %-12s  %s\n" "$$ok" "venv" "$(venv_dir)  ($$($(PYTHON) --version 2>&1))"; \
	  venv_py="$(PYTHON)"; \
	else \
	  printf "  %s  %-12s  run: make venv\n" "$$bad" "venv"; \
	  missing=$$((missing+1)); \
	  venv_py="python3"; \
	fi; \
	check_py_import "$$venv_py" pybind11 "make venv  (or: pip install pybind11==2.9.2)"; \
	check_py_import "$$venv_py" pytest   "make venv  (or: pip install pytest)"; \
	\
	echo ""; \
	echo "  ── Optional (make package / make publish) ──────────────────────"; \
	check_opt docker  "https://docs.docker.com/engine/install/"; \
	check_opt twine   "pip install twine"; \
	\
	echo ""; \
	if [ "$$missing" -gt 0 ]; then \
	  echo "  ✗  $$missing required dependency/ies missing (see [MISSING] above)."; \
	  echo ""; \
	  exit 1; \
	else \
	  echo "  ✓  All required dependencies found."; \
	  echo ""; \
	fi

# ---------------------------------------------------------------------------
# Virtual environment
# ---------------------------------------------------------------------------
# Creates .venv and installs all Python build + test dependencies.
# Re-runs whenever pyproject.toml changes.
$(venv_dir): pyproject.toml
	python3 -m venv $(venv_dir)
	$(PIP) install --upgrade pip
	$(PIP) install "pybind11==2.9.2" scikit-build setuptools mypy pytest
	@touch $(venv_dir)

.PHONY: venv
venv: $(venv_dir)

# ---------------------------------------------------------------------------
# C++ / cmake targets
# ---------------------------------------------------------------------------

# Configure cmake.  Passes the venv python and its pybind11 cmake-dir so
# find_package(pybind11) works without SKBUILD.
.PHONY: cmake/configure
cmake/configure: $(venv_dir)
	@cmake -B $(build_dir) -DCMAKE_BUILD_TYPE=Release \
	    -DPYTHON_EXECUTABLE=$(PYTHON) \
	    -DCMAKE_PREFIX_PATH=$$($(PYTHON) -c "import pybind11; print(pybind11.get_cmake_dir())")

# Build everything cmake knows about (Python module + static lib)
.PHONY: cmake/build
cmake/build: cmake/configure
	@cmake --build $(build_dir) --parallel

# ---------------------------------------------------------------------------
# Rust targets
# ---------------------------------------------------------------------------
# The static lib is installed alongside the Python package by `make install`.
# We locate it through the venv's Python so that no separate cmake run is needed.
# build.rs honours MOMENTUMX_BUILD_DIR; we set it here to the installed lib dir.
mx_lib_dir = $(shell $(PYTHON) -c \
    "import pathlib, momentumx; \
     print(pathlib.Path(momentumx.__file__).parent / 'lib')" 2>/dev/null)

# Passed as a prefix to every cargo invocation.  Empty string when momentumx
# is not yet installed (build.rs will fall back to ../build and report clearly).
cargo_env = $(if $(mx_lib_dir),MOMENTUMX_BUILD_DIR="$(mx_lib_dir)")

.PHONY: rust/build
rust/build:
	@cd $(rust_dir) && $(cargo_env) cargo build

.PHONY: rust/build/release
rust/build/release:
	@cd $(rust_dir) && $(cargo_env) cargo build --release

.PHONY: rust/build/examples
rust/build/examples:
	@cd $(rust_dir) && $(cargo_env) cargo build --release --examples

.PHONY: rust/test
rust/test:
	@cd $(rust_dir) && $(cargo_env) cargo test

.PHONY: rust/test/disable_condition
rust/test/disable_condition:
	@cd $(rust_dir) && $(cargo_env) MX_DISABLE_CONDITION=1 cargo test

# ---------------------------------------------------------------------------
# Packaging (Docker-based manylinux wheels)
# ---------------------------------------------------------------------------

.PHONY: package_wheel_%
package_wheel_%:
	@rm -rfv dist/*-$*-*.whl
	@docker run $(docker_build_args) /opt/python/$*/bin/python3 -m pip wheel -w /io/dist .
	@rm -rfv _skbuild

.PHONY: package_wheels
package_wheels: package_wheel_cp38-cp38 package_wheel_cp39-cp39 package_wheel_cp310-cp310 package_wheel_cp311-cp311 package_wheel_cp312-cp312 package_wheel_cp313-cp313

.PHONY: package
package: test clean package_wheels
	@for WHEEL in dist/*.whl; do \
		docker run $(docker_build_args) auditwheel repair $(auditwheel_args) /io/$$WHEEL -w /io/dist; \
		rm -fv $$WHEEL; \
	done
	@$(PYTHON) setup.py sdist

.PHONY: publish
publish:
	@twine upload dist/* --verbose

# ---------------------------------------------------------------------------
# Install / test
# ---------------------------------------------------------------------------

.PHONY: install
install: $(venv_dir)
	@$(PIP) uninstall --yes momentumx || echo "Nothing to remove"
	@($(PIP) install .[test] --use-feature=in-tree-build --no-build-isolation -v) || \
	 ($(PIP) install .[test] --no-build-isolation -v)
	@cp -uv _skbuild/linux-x86_64*/cmake-install/src/momentumx/_mx.pyi src/momentumx/_mx.pyi

# Run Python + Rust unit tests + cross-language integration tests
.PHONY: test
test: test/python rust/test test/crosslang

.PHONY: test/crosslang
test/crosslang: $(venv_dir) rust/build/examples
	@$(PYTHON) -m pytest tests/test_crosslang.py -v

.PHONY: test/python
test/python: $(venv_dir)
	@rm -rfv /dev/shm/mx.test_echo_mx_stream*
	@$(PYTHON) -m pytest tests --ignore=tests/test_crosslang.py

.PHONY: test/disable_condition
test/disable_condition: $(venv_dir)
	@rm -rfv /dev/shm/mx.test_echo_mx_stream*
	@MX_DISABLE_CONDITION=1 $(PYTHON) -m pytest tests
	@cd $(rust_dir) && MX_DISABLE_CONDITION=1 cargo test

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------

.PHONY: clean
clean:
	@rm -rfv dist
	@rm -rfv _skbuild
	@rm -rfv $(build_dir)
	@rm -rfv $(venv_dir)
	@cd $(rust_dir) && cargo clean
