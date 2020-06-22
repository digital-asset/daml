.PHONY: build test start-frontend start-ui-core

default: build

build:
	bazel build //navigator/...

test:
	bazel test //navigator/...

start-backend:
	$(MAKE) -C backend start

start-frontend:
	$(MAKE) -C frontend start

start-ui-core:
	$(MAKE) -C frontend start-ui-core

# CI: Makes sure the frontend code is nicely formatted.
lint:
	$(MAKE) -C frontend lint

# CI: Makes sure every file has a license header.
check-licenses:
	dade-copyright-headers check navigator
