.PHONY: setup build start clean setup update-graphql-types report-licenses

default: build

setup:
	# Always install dev dependencies, since almost all
	# commands require some build step.
	yarn install --production=false

build: clean setup update-graphql-types
	yarn build

test: setup
	yarn test

start: setup
	yarn start

start-ui-core: setup
	yarn start-ui-core

clean:
	rm -rf dist/

lint: setup
	yarn lint

report-licenses: setup
	yarn license-checker --production --json --out $(OUT)

update-graphql-types: setup
	yarn run apollo client:codegen -c ./apollo.config.js --passthroughCustomScalars --customScalarsPrefix=OpaqueTypes. --outputFlat --target typescript --includes="./src/ui-core/**/*.ts*" ./src/ui-core/src/api/QueriesBody.txt
	echo 'import * as OpaqueTypes from "./OpaqueTypes";' > ./src/ui-core/src/api/Queries.ts
	cat ./src/ui-core/src/api/QueriesBody.txt >> ./src/ui-core/src/api/Queries.ts
	rm ./src/ui-core/src/api/QueriesBody.txt

	yarn run apollo client:codegen -c ./apollo.config.js --passthroughCustomScalars --customScalarsPrefix=OpaqueTypes. --outputFlat --target typescript --includes="./src/**/*.ts*" --excludes="./src/ui-core/**/*.ts*" ./src/api/QueriesBody.txt
	echo 'import * as OpaqueTypes from "@da/ui-core/lib/api/OpaqueTypes";' > ./src/api/Queries.ts
	cat ./src/api/QueriesBody.txt >> ./src/api/Queries.ts
	rm ./src/api/QueriesBody.txt
