.PHONY: clean build publish bundle-frontend test

default: build

clean:
	rm -rf dist

build: clean bundle-frontend
	$(MAKE) -C backend build
	mkdir -p dist
	mv backend/target/navigator-*.jar dist/
	$(MAKE) -C integration-test build

publish: bundle-frontend
	$(MAKE) -C backend publish
	$(MAKE) -C integration-test publish

bundle-frontend:
	$(MAKE) -C frontend build
	rm -rf backend/src/main/resources/frontend
	mv frontend/dist backend/src/main/resources/frontend

test:
	$(MAKE) -C backend test
	$(MAKE) -C frontend test
	$(MAKE) -C integration-test test

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


