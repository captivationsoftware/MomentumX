.PHONY: all
all: test

.PHONY: build
build:
	@rm -rfv dist/momentumx-*.whl
	@python3 setup.py bdist_wheel
	@WHEEL_FILE=`ls dist/*.whl`; \
	docker run --rm -v `pwd`:/io quay.io/pypa/manylinux_2_28_x86_64:latest auditwheel repair --plat manylinux_2_28_x86_64 /io/$$WHEEL_FILE -w /io/dist; \
	rm $$WHEEL_FILE

.PHONY: clean
clean:
	@rm -rfv dist
	@rm -rfv _skbuild

.PHONY: install
install: build
	@python3 -m pip uninstall --yes momentumx || echo "Nothing to remove"
	@python3 -m pip install -e .

.PHONY: test
test: install
	@pytest tests

# .PHONY: rocky8
# rocky8:
# 	@docker build . -f package/Dockerfile.rocky8 -t momentum:rocky8

# .PHONY: rocky9
# rocky9:
# 	@docker build . -f package/Dockerfile.rocky9 -t momentum:rocky9

# .PHONY: jammy
# jammy:
# 	@docker build . -f package/Dockerfile.jammy -t momentum:jammy

# .PHONY: kinetic
# kinetic:
# 	@docker build . -f package/Dockerfile.kinetic -t momentum:kinetic
