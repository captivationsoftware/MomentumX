docker_build_args = --rm \
       -u `id -u`:`id -g` \
       -v `pwd`:/io \
       -w /io \
       quay.io/pypa/manylinux2014_x86_64:latest
auditwheel_args = --plat manylinux2014_x86_64

.PHONY: all
all: install

.PHONY: clean
clean:
	@rm -rfv dist
	@rm -rfv _skbuild

.PHONY: package_wheel_%
package_wheel_%:
	@rm -rfv dist/*-$*-*.whl
	@docker run $(docker_build_args) /opt/python/$*/bin/python3 -m pip wheel -w /io/dist .
	@rm -rfv _skbuild


.PHONY: package_wheels
package_wheels: package_wheel_cp36-cp36m package_wheel_cp37-cp37m package_wheel_cp38-cp38 package_wheel_cp39-cp39 package_wheel_cp310-cp310 package_wheel_cp311-cp311

.PHONY: package
package: test clean package_wheels
	@for WHEEL in dist/*.whl; do \
		docker run $(docker_build_args) auditwheel repair $(auditwheel_args) /io/$$WHEEL -w /io/dist; \
		rm -fv $$WHEEL; \
	done
	@python3 setup.py sdist

.PHONY: publish
publish: 
	@twine upload dist/* --verbose

.PHONY: install
install: 
	@python3 -m pip uninstall --yes momentumx || echo "Nothing to remove"
	@(python3 -m pip install .[test] --use-feature=in-tree-build --no-build-isolation -v) || (python3 -m pip install .[test] --no-build-isolation -v)
	@cp -uv _skbuild/linux-x86_64*/cmake-install/src/momentumx/_mx.pyi src/momentumx/_mx.pyi

.PHONY: test
test:
	@rm -rfv rm -rf /dev/shm/mx.test_echo_mx_stream*
	@python3 -m pytest tests

.PHONY: test/disable_condition
test/disable_condition:
	@rm -rfv rm -rf /dev/shm/mx.test_echo_mx_stream*
	@MX_DISABLE_CONDITION=1 python3 -m pytest tests
