.PHONY: test test-204 test-212 test-213
test:
	docker run --rm \
		--volume $$PWD/../../../:/code \
		--workdir /code \
		ubuntu \
		dev-env/tests/docker/test.sh

test-212:
	docker run --rm \
		--volume $$PWD/../../../:/code \
		--workdir /code \
		ubuntu \
		dev-env/tests/docker/test.sh 2.1.2

test-213:
	docker run --rm \
		--volume $$PWD/../../../:/code \
		--workdir /code \
		ubuntu \
		dev-env/tests/docker/test.sh 2.1.3
