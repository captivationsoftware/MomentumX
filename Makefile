.PHONY: all
all: test

.PHONY: build
build:
	@rm -rfv dist
	@python3 setup.py --verbose bdist_wheel

# TODO: pyproject.toml-based build
.PHONY: alt
alt:
	@python3 -m pip wheel build . -w dist

.PHONY: clean
clean:
	@rm -rfv dist
	@rm -rfv _skbuild

.PHONY: install/dev
install/dev:
	@python3 -m pip install .

.PHONY: install/local
install/local: build
	@cd /tmp && python3 -m pip uninstall --yes momentumx || echo "Nothing to remove"
	@python3 -m pip install dist/momentumx-*.whl

.PHONY: test
test: install/local
	@cd /tmp && python3 -c "import momentumx"

z_ignore:
	@python3 -m pytest
	@find . -iname '*.so'

