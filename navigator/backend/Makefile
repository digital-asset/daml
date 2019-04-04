.PHONY: clean build publish test start
.PHONY: publish-backend publish-docs navigator-docs

version:=$(shell awk -F\" '/^[ \t]*version/ { print $$2 }' < build.sbt)

default: build

clean:
	sbt -no-colors clean

build:
	sbt -no-colors assembly

publish: publish-backend publish-docs

publish-backend:
	sbt -no-colors publish

# To publish documentation, you will need Bintray credentials
# with publish rights (ask Jussi) and the JFrog configurations.
# Run 'jfrog bt config' to set it up. Jenkins has the JFrog config
# available via the 'withJFrogCredentials' helper.
publish-docs: navigator-docs
	jfrog bt upload --publish=true --flat=false \
	  navigator-docs-${version}.tar.gz \
	  digitalassetsdk/DigitalAssetSDK/navigator/${version} \
	  com/digitalasset/navigator/navigator-docs/${version}/navigator-docs-${version}.tar.gz
	rm -f navigator-docs-${version}.tar.gz

test:
	sbt -no-colors test

start:
	sbt -no-colors "run server"

report-licenses:
	sbt dumpLicenseReport
	cp target/license-reports/navigator-licenses.csv $(OUT)
