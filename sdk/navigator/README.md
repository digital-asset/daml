Navigator
=========

The *Navigator* is a web-app that connects to any Digital Asset ledger and
allows the user to inspect contracts, create contracts, and exercise choices.

The Navigator can be used in development mode (see below) or packaged into a
"fat" JAR that includes the compiled frontend assets for distribution.

Navigator architecture
----------------------

To learn more about developing the different parts of the Navigator see:

- [Navigator Frontend README](./frontend)
- [Navigator Backend README](./backend)

Building Navigator
------------------

To build a "fat" JAR of the Navigator that includes the pre-compiled front-end
assets, run:

```bash
bazel build //navigator/backend:navigator-binary_deploy.jar
```

This produces a "fat" JAR `bazel-bin/navigator/backend/navigator-binary_deploy.jar` which can be run with:

```bash
java -jar bazel-bin/navigator/backend/navigator-binary_deploy.jar
```

Notable things in the Navigator build:

### backend/src/test/resources/schema.graphql

Manually written, must be consistent with `backend/src/main/scala/com/digitalasset/navigator/graphql/GraphQLSchema.scala`. Consistency is checked in a test.

### frontend/src/**/api/Queries.ts

Generated from `backend/src/test/resources/schema.graphql` with an external codegen tool.
Currently, these files are checked in and updated with `make update-graphql-types`.

### frontend bundled code

Code from `frontend/src/**/*.ts*`, compiled using TypeScript, and bundled with Webpack.
Output includes:
- `bundle-[hash].js`: bundled frontend code, name uses content hasing.
- `browsercheck-[hash].js`: tiny module for checking browser compatibility, name uses content hasing.
- Several image and font files, referenced by the above modules. File names use content hashing.
- `index.html`: Single page application main entry, references the above modules.

Note: Browsers are instructed never to cache `index.html`, and indefinitely cache all other files. This is why content hashing is used.

### backend binary

Scala binary, compiled as a fat JAR.
Code from `backend/src/**/*.scala`, bundled frontend code is copied to `backend/src/main/resources/frontend`.

### backend version

The version is included as resource files in the Navigator fat jar.
This is to reduce rebuild times when the version changes.

### frontend development build

For developing frontend code, `webpack-dev-server` is used. This serves the current frontend code on a separate port, and does:
- Watch `*.ts` files for changes
- Perform incremental builds
- Send a push notification to the browser, automatically reloading the page when the build is finished.
- Forward network requests to a different port, where a Navigator backend is expected to run.

This is orders of magnitude faster than what the current Bazel build offers, so it is desirable to keep the `webpack-dev-server` setup working. 

Note, the browser is instructed to cache assets based on the SDK version.
During development this is too aggressive and you will need to manually refresh to see updates to the front-end.
