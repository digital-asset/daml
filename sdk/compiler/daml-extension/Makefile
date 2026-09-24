
all: out/src/extension.js

out/src/extension.js: node_modules types src/extension.ts
	yarn run compile

node_modules:
	yarn install

.PHONY: types
types: node_modules/vscode/vscode.d.ts node_modules/vscode/thenable.d.ts

node_modules/vscode/vscode.d.ts:
	mkdir -p node_modules/vscode && cp -vf vendor/vscode.d.ts node_modules/vscode/

node_modules/vscode/thenable.d.ts:
	mkdir -p node_modules/vscode && cp -vf vendor/thenable.d.ts node_modules/vscode/

