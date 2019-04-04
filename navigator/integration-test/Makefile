.PHONY: clean build publish test start
.PHONY: publish-backend publish-docs navigator-docs

default: build

clean:
	sbt -no-color clean

build:
	sbt -no-color assembly

publish:
	sbt -no-color publish

test:
	./test-local-navigator.sh
