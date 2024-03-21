Redux state/action router
=========================

This library provides Redux middleware for handling routing in a single-page
application using the History API. The idea is very simple:

- The current path is a function of the Redux state.
- When the path changes (and when the app is first loaded), dispatch a Redux
  action that is a function of the path.

This should seem familiar since it's the same way that React relates to Redux:
the rendered interface is a function of the Redux state and interactions with
the interface generates actions. In other words, we treat the URL bar as a
special user interface component, which is what it really is:

![This middleware treats the URL bar as a special UI component by using the
History API. It listens to path changes and sets the path to correspond to the
current Redux state](docs/state-action-flow.png)

The advantage of this is that your URL bar fits in perfectly with your existing
Redux workflow. The URL is guaranteed to be consistent with the current Redux
state and navigating the browser history results in normal actions and
introduces no new concepts that you need to keep in mind.

Usage
-----

The library uses the private `@da` scope and is published to the private Digital
Asset NPM registry.

You need to provide two functions as options to the router. They define how we
render a URL given a state and what action to dispatch for a given path:

```typescript
type URL = string;

interface RouterOptions<S> {
  stateToUrl(state: S): URL;
  urlToAction(url: URL): Action;
}

type RouterMiddleware<S> = (options: RouterOptions<S>) => Middleware;
```

For example:

```typescript
import * as Router from '@da/redux-router';
import myApp from './app';
import { stateToUrl, urlToAction } from './routes';

const store = createStore(
  myApp,
  applyMiddleware(Router.middleware({ stateToUrl, urlToAction }))
);
```

Note that this library does not control page scrolling between path changes. It
also does not currently support query strings in the URL. These are features we
might develop in the future.
