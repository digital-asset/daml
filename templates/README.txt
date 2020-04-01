These are template projects for `daml new`.

Testing the create-daml-app template
====================================

While automated integration tests for the create-daml-app template are being built,
we have the following manual testing procedure.

First, build the SDK from head using the `daml-sdk-head` command.
This gives an executable DAML assistant called `daml-head` in your path.

Next, instantiate the `create-daml-app` template as follows:

```
daml-head new create-daml-app
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
        "daml-ts",
        "ui"
    ]
}

Now you can continue to build and run the project as described in create-daml-app/README.md,
using `daml-head` instead of `daml` everywhere.
After following those instructions and getting the app up and running, also make sure the
UI tests work using `yarn test` in the `ui` folder.

