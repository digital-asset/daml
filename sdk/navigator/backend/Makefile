.PHONY: build test start

default: build

build:
	bazel build //navigator/backend/...

test:
	bazel test //navigator/backend/...

report-licenses:
	sbt dumpLicenseReport
	cp target/license-reports/navigator-licenses.csv $(OUT)
