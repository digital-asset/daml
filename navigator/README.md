Navigator
=========

The *Navigator* is a web-app that connects to any Digital Asset ledger and
allows the user to inspect contracts, create contracts, and exercise choices.

The Navigator can be used in development mode (see below) or packaged into a
"fat" JAR that includes the compiled frontend assets for distribution.

Building Navigator
------------------

To build a "fat" JAR of the Navigator that includes the pre-compiled front-end
assets, run:

```bash
bazel build //navigator:navigator-binary_deploy.jar
```

This produces a "fat" JAR `dist/navigator-x.x.x.jar` which can be run with:

```bash
java -jar dist/navigator-x.x.x.jar
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

### frontend commit and version

The current git commit and the Navigator version is hardcoded in the bundled frontend code.
As part of the build process, Webpack replaces the `__BUILD_VERSION__` and `__BUILD_COMMIT__` identifiers in the source code by values specified in `webpack.config.js`.

### backend binary

Scala binary, compiled as a fat JAR.
Code from `backend/src/**/*.scala`, bundled frontend code is copied to `backend/src/main/resources/frontend`.

### backend commit and version

The backend uses the `"com.eed3si9n" % "sbt-buildinfo"` plugin to generate a Scala file containing the app version and build commit.

### frontend development build

For developing frontend code, `webpack-dev-server` is used. This serves the current frontend code on a separate port, and does:
- Watch `*.ts` files for changes
- Perform incremental builds
- Send a push notification to the browser, automatically reloading the page when the build is finished.
- Forward network requests to a different port, where a Navigator backend is expected to run.

This is orders of magnitude faster than what the current Bazel build offers, so it is desirable to keep the `webpack-dev-server` setup working. 

Publishing Navigator
--------------------

To publish a "fat" JAR of the Navigator that includes the pre-compiled front-end
assets, bump the version in `backend/build.sbt` and then run:

```bash
make publish
```

This uploads a "fat" JAR to the `libs-releases-local` repo on Artifactory.

Deploying Navigator
-------------------

To deploy Navigator on a server, first get a "fat" JAR using one of the
following options:
- Build Navigator from source (see above)
- Download Navigator from the `libs-releases-local` repo on Artifactory
- Download the Digital Asset SDK, which includes the Navigator

Once you have the JAR file, run it as described in the
[Building Navigator](#Building-Navigator) section.

Navigator Development
---------------------

To modify the Navigator (frontend) you need to set up the same Artifactory
access as per the UI framework section above. You also need to run both a ledger
and the backend as per above. To then run the Navigator in development mode on
`http://localhost:8000/`:

  ```bash
  cd frontend
  yarn install
  yarn start
  ```

Navigator architecture
---------------------

To learn more about developing the different parts of the Navigator see:

- [Navigator Frontend README](./frontend)
- [Navigator Backend README](./backend)