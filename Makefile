docker_build_args = --rm \
	-u `id -u`:`id -g` \
	-v `pwd`:/io \
	-w /io \
	quay.io/pypa/manylinux_2_28_x86_64:latest
auditwheel_args = --plat manylinux_2_28_x86_64

.PHONY: all
all: test

.PHONY: build_dist_%
build_dist_%:
	@rm -rfv dist/*-$*-*.whl
	@docker run $(docker_build_args) /opt/python/$*/bin/python3 -m pip wheel -w /io/dist .
	@rm -rfv _skbuild

.PHONY: build_dists
build_dists: build_dist_cp37-cp37m build_dist_cp38-cp38 build_dist_cp39-cp39 build_dist_cp310-cp310 build_dist_cp311-cp311
	@docker run $(docker_build_args) /opt/python/cp310-cp310/bin/python3 -m build --sdist

.PHONY: build
build: build_dists
	@for WHEEL in dist/*.whl; do \
		docker run $(docker_build_args) auditwheel repair $(auditwheel_args) /io/$$WHEEL -w /io/dist; \
	done

.PHONY: clean
clean:
	@rm -rfv build
	@rm -rfv dist

.PHONY: install
install: build
	@python3 -m pip uninstall --yes momentumx || echo "Nothing to remove"
	@python3 -m pip install -e .[tests]

.PHONY: test
test: install
	@python3 -m pytest tests

