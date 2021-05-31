These are template projects for `daml new`.

Testing the create-daml-app template
====================================

While automated integration tests for the create-daml-app template are being built,
we have the following manual testing procedure.
Note that this is for testing against the head of the daml repo.
For testing against a released SDK, you can skip past the package.json step and use
`daml` instead of `daml-head` after installing the released version.


First, build the SDK from head using the `daml-sdk-head` command.
This gives an executable Daml assistant called `daml-head` in your path.

Next, instantiate the `create-daml-app` template as follows:

```
daml-head new create-daml-app --template create-daml-app
cd create-daml-app
```

Crucially, you'll need to add a package.json file at the root of the project for testing
(this is not required when using the released SDK).
It should look as follows, with the dummy paths here replaced by relative paths to locally
built TypeScript libraries.
(These need to be built from head using Bazel:
```
bazel build //language-support/ts/daml-types
bazel build //language-support/ts/daml-ledger
bazel build //language-support/ts/daml-react```)

package.json:
{
    "resolutions": {
        "@daml/types": "file:path/to/daml-types/npm_package",
        "@daml/ledger": "file:path/to/daml-ledger/npm_package",
        "@daml/react": "file:path/to/daml-react/npm_package"
    },
    "private": true,
    "workspaces": [
        "daml.js",
        "ui"
    ]
}

Now you can continue to build and run the project as described in create-daml-app/README.md,
using `daml-head` instead of `daml`.
Specifically, you should run the following in the root directory:
```
daml-head build
daml-head codegen js .daml/dist/create-daml-app-0.1.0.dar -o daml.js
daml-head start
```

Then in another terminal, navigate to `create-daml-app/ui/` and run:
```
yarn install
yarn start
```
And check that the app works.

Finally, terminate both the `daml start` and `yarn start` processes and run
`yarn test` from the `ui` directory. All tests should pass.

