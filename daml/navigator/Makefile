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

# Updates licensing documentation.
# This documentation includes licensing info for each library that we use.
update-license-docs:
	(cd docs/licenses && ./extract-sbt.py)
	(cd docs/licenses && ./extract-js.py)

# CI: Makes sure the frontend code is nicely formatted.
lint:
	$(MAKE) -C frontend lint

# CI: Makes sure every file has a license header.
check-licenses:
	dade-copyright-headers check navigator

# CI: Makes sure licensing documentation is up to date.
check-license-docs:
	(cd docs/licenses && ./extract-sbt.py check)
	(cd docs/licenses && ./extract-js.py check)


