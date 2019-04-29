Core UI Libraries
=================

This is a collection of core UI libraries for writing applications on
the ledger.

Components
==========

Navigator components library
----------------------------

This library contains React components intended to be used by ledger frontend
apps. The components have TypeScript declarations and they use
[`styled-component`](https://www.styled-components.com/) for styling, which
affords customisation and theming.

### Usage

To use these components you need to do two things:

- Provide theme variables to components by wrapping your app in a
  `ThemeProvider`.
- Include an icon font in your app that includes at least the icons [defined in
  the `IconName` type](./src/Icon.tsx) together with global, prefixed CSS
  classnames for using them.

#### Theme

To use components, your app needs to be wrapped in a `ThemeProvider` that
provide components with theme variables (via React's context feature). You can
provide a full theme or override individual variables in the default theme using
ES6 spread operator or `Object.assign`. For example:

```typescript
import * as ReactDOM from 'react-dom';
import { defaultTheme, ThemeProvider } from '@da/ui-core';

const theme: ThemeInterface = {
  ...defaultTheme,
  colorPrimary: 'blue',
};

ReactDOM.render(
  <ThemeProvider theme={theme}>
    <YourApp />
  </ThemeProvider>,
  document.getElementById('app'),
);
```

#### Icons

This library does not ship with any icons in order to simplify the build and
packaging for apps, which will almost always want to customise and add
additional icons. Instead, *the app is assumed to provide an icon font and
global, prefixed CSS class names for using them*, `icon-arrow-right` or
`icon-user` for example.

The icons assumed to exist by components in the library are defined in the
[`IconName` type](./src/Icon.tsx). For example, `chevron-right` means the icon
font and corresponding CSS should provide a `icon-chevron-right` icon when the
default prefix, `icon-`, is used. The prefix can be customised by the
`iconPrefix` variable in the theme. Setting it to `fancy-` means the icon font
CSS needs to define `fancy-chevron-right` instead.

Providing icons through an icon font in this way is common and powerful, used by
popular icon sets such as [Font Awesome](http://fontawesome.io/). Several web
apps also exist to let you create your own custom icon fonts
([Fontastic](http://fontastic.me/), [IcoMoon](https://icomoon.io/), and
[Fontello](http://fontello.com/) for example).

You don't have to provide icons for components that you don't use, though at the
moment there is no easy way to find out which components use which icons. If you
use a component that uses an icon that is not in your icon font or corresponding
classes, it will simply not show up where it should.

#### Components

You can then import components individually or from `@da/ui-core`:

```typescript
import { Button } from '@da/ui-core';
// import Button from '@da/ui-core/lib/Button';

const MyComponent = <div>
  <Button>Press me!</Button>
</div>
```


Ledger Watcher
--------------

This library contains a utility that apps can use to watch the state of the
ledger. It can be configured to watch:

- Archiving of particular contracts. This is useful when one or more contracts
  are being shown on the screen and you want to ensure they aren't shown as
  active long after they have been archived.
- Command success/failure. This is useful to track and follow up on a submitted
  command.

### Usage

To use the watcher, instantiate and start a single watcher in your app (usually
in your main `index` file) with your Apollo `client`, your Redux `store`, and
the appropriate function to dispatch a watcher action:

```typescript
const watcher = new Watcher(
  client,
  () => (store.getState().watcher),
  (watcherAction: WatcherAction) => {
    store.dispatch(Actions.toWatcher(watcherAction));
  },
);
watcher.start();
```

Then send `register(componentId: string, ids: ContractId[])` and
`unregister(componentId: string)` actions to the watcher to watch contract
archivals and `registerCommand(businessIntent: string)` to watch command
execution. Exactly what this looks like depends on your app, but it might look
like this:

```typescript
import { register, registerCommand } from '@da/ui-core/ledger-watcher';

dispatch(Actions.toWatcher(register(componentId, contractIds))));
dispatch(Actions.toWatcher(unregister(componentId))));
dispatch(Actions.toWatcher(registerCommand(businessIntent)));
```

This will keep the contracts' archival state up-to-date in the Apollo store and
add the command states to the watcher state's `commands` field in the Redux
state.

Route Matcher
-------------

This library defines a `Route` class for creating routes and a way to combine
routes into functions that an application can use. It is designed to be used
with pure state- and action-based URL handling as implemented in
`@da/redux-router`.

### Usage

An application should define a route for each URL pattern it uses and then use
these in links and for History API handling. This gives the application a single
point of truth for the URL patterns and co-locates the definition of the URL
pattern, the action that should be fired when the URL is matched, and how to
extract the URL parameters from the state. When using TypeScript it also
provides the benefit of more type safe routes.

```typescript import { combineRoutes, Route } from '@da/ui-core'; import {
Action, State, Page, setPageAction } from './state'; import * as List from
'../contract';

const setPageAction = (page: Page) => { type: 'SET_PAGE', page };

type TodoParams = { id: string };

export const todo = new Route<TodoParams, Action, State>(
  '/todos/:id(/)',
  ({ id }) => setPageAction({ type: 'todo', id }),
  ({ activeTodoId }: State) =>
    (activeTodoId !== null ? { id: activeTodoId } : undefined),
);

export const todos = new Route<{}, Action, State>(
  '/todos(/)',
  () => setPageAction({ type: 'todos' }),

  // Here we say that if no todo is active, show the list of todos, by returning
  // a successful match, indicated by returning an object of parameters for this
  // route, which is an empty object since the route doesn't have any
  // parameters.

  ({ activeTodoId }: State) => (activeTodoId === null ? {} : undefined)
);

const anything = new Route<{}, Action, State>(
  '/*anything',
  () => set({ type: 'todos' }),
  (_: State) => ({}),
);

// Now we combine these routes into functions that we can use to set the path
// (based on the current state), generate an action from a user changing the
// URL, and finding what route object is currently active (useful to
// conditionally style menu options for example).

export const { stateToPath, pathToAction, activeRoute } = combineRoutes([
  todo,
  todos,
  anything,
]);
```

### `Router`

A `Route<Params, Action, State>` encapsulates:

- The path pattern and the parameters used by it.
- What action going to the path should result in.
- How to extract the route parameters from the state.

```typescript
class Route<P, A, S> {
  constructor(
    pattern: string,
    createAction: (params: P) => A | undefined,
    extractParams: (state: S) => P | undefined,
  )
  render(params: P): Path;
  match(path: Path): A | undefined;
  extractParams: (state: S) => P | undefined;
}
```

### `combineRoutes`

`combineRoutes` returns a set of utility functions for matching and rendering a
set of routes. These can be passed to libraries like `@da/redux-router`.

```typescript
function combineRoutes<P, A, S>(routes: Route<P, A, S>[]): {
  // Given a state, what's the current path?
  stateToPath(state: S): Path;

  // Given a Path change, what action should be sent?
  pathToAction(path: Path): A;

  // What's the active route?
  activeRoute(): Route<P, A, S>;
}
```

### Improvements

It would be nicer to design an API that composes in a tree structure. That is,
we would have a single top-level "router" which is a composition of a page
router and a session router. These may then in turn compose other routers. Each
router for a particular state/action type: the top-level router takes and
generates the top-level state/action types and so on. Routers would all expose:

- `toPath(state: State): Path`
- `toAction(path: Path): Action | undefined`

It also needs some way to expose functions to render particular routes given
arguments (for links for example). I'm not sure what the API for that should be
yet however. In the current implementation we get it for free by each route
being a global, typed, variable. Here we'd need to somehow expose it
hierarchically from the router objects probably.
