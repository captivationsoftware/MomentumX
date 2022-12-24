.PHONY: all
all: test

.PHONY: build
build:
	@rm -rfv dist/momentumx-*.whl
	@python3 setup.py bdist_wheel

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
