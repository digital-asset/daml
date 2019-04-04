BUILD_DIR := demo/build/lib.linux-x86_64-3.6
DEMO_SRC := demo/setup.py $(wildcard demo/demo/*.py) $(wildcard demo/demo/*.c)

# Warning: the paths on Jenkins are so deeply nested the shebang line
# in the generated pip script exceeds the kernel limit of 128 bytes.
# However, we can invoke Pip in this way instead.
.PHONY: test-pip
test-pip:
	python3 -m venv --clear proj
	proj/bin/python proj/bin/pip install --only-binary :all: pandas matplotlib

.PHONY: test-loading
test-loading:
	python3 check_manylinux1.py

.PHONY: test-extension
test-extension: $(BUILD_DIR)
	cd $(BUILD_DIR) \
		&& python3 -c 'from demo import ext; print(ext.isotime())'

$(BUILD_DIR): $(DEMO_SRC)
	docker run --rm \
		--user $$(id -u):$$(id -g) \
		--volume $$PWD/demo:/demo \
		--workdir /demo \
		quay.io/pypa/manylinux1_x86_64 \
		/opt/python/cp36-cp36m/bin/python setup.py build
