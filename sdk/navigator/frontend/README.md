Navigator Frontend
=================

The Navigator frontend is based on the [ui-core](./src/ui-core/src) library.

Developing Navigator Frontend
-----------------------------

To build the bundled frontend files, use Bazel:
```bash
bazel build //navigator/frontend:frontend.jar
```

To test the frontend during development, start the backend at port 4000 (defined in [webpack.config.js](./webpack.config.js)), run the following command, and open a web browser at the address indicated in the command output:
```bash
make start
```

This will start an interactive build using `webpack-dev-server`.
Every time you save a file, the frontend assets will be rebuilt, and a push notification will be sent to the browser, prompting it to reload the page.

See the ui-core [README](./src/ui-core/README.md) for information about the design of some of the core components of Navigator.


Configurable table views
------------------------

Configurable table views are a rapid prototyping feature,
where developers can write a script that returns a list of custom table views for a given user.

## Architecture

- The [configsource](./src/applets/configsource) applet is responsible for loading the config file.
  - The config file source is loaded from the backend
    [/api/config](http://localhost:8000/api/config) endpoint.
  - The config file source is stored in the state of the applet.
- The config file is parsed and evaluated in the [app](./src/applets/app) UI component.
  - The evaluated config is stored in the component state, and only re-evaluated if either
    the current user or the config file source has changed.
  - The evaluated config is passed as a property to child components.
- The [customview](./src/applets/customview) applet implements the rendering of custom views.
  - This applet forwards all functionality to either the `Contracts`, `Templates`,
    or `TemplateContracts` applets.
  - The route for custom views only contains its ID.
    Therefore, the state of the applet initially only holds the ID of the custom view.
  - When the applet UI component is rendered for the first time,
    it initializes the state of the child applet (it now has access to the evaluated config,
    passed as props from the [app](./src/applets/app) UI component).
- Config file parsing and loading is implemented in the [config](./src/config) module.
  - The config file is first preprocessed using Babel (to support JSX tags and ES6 code).
  - Import statements are implemented using a custom `require` function,
    that only provides modules already bundled with the Navigator.
  - The parsed config file is checked for its version.
  - There is generally little run time validation.
