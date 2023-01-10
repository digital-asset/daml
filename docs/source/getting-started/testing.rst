.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Testing Your Web App
********************

When developing a UI for your Daml application, you will want to test that user flows work from end to end.
This means that actions performed in the web UI trigger updates to the ledger and give the desired results on the page.
In this section we show how you can do such testing automatically in TypeScript (equally JavaScript).
This will allow you to iterate on your app faster and with more confidence!

There are two tools that we chose to write end to end tests for our app.
Of course there are more to choose from, but this is one combination that works.

    - `Jest <https://jestjs.io/>`_ is a general-purpose testing framework for JavaScript that's well integrated with both TypeScript and React. Jest helps you structure your tests and express expectations of the app's behaviour.
    - `Puppeteer <https://pptr.dev/>`_ is a library for controlling a Chrome browser from JavaScript/TypeScript. Puppeteer allows you to simulate interactions with the app in place of a real user.

To install Puppeteer and some other testing utilities we are going to use,
run the following command in the ``ui`` directory::

    npm i --save-dev puppeteer@~10.0.0 wait-on@~6.0.1 @types/jest@~29.2.3 @types/node@~18.11.9 @types/puppeteer@~7.0.4 @types/wait-on@~5.3.1

You may need to run ``npm install`` again afterwards.

Because these things are easier to describe with concrete examples, this
section will show how to set up end-to-end tests for the application you would
end with at the end of the :doc:`/getting-started/first-feature` section.

Set Up the Tests
================

Let's see how to use these tools to write some tests for our social network app.
You can see the full suite in section :ref:`Full Test Suite` at the bottom of
this page.
To run this test suite, create a new file ``ui/src/index.test.ts``, copy the
code in this section into that file and run the following command in the ``ui``
folder::

    npm test

The actual tests are the clauses beginning with ``test``.
You can scroll down to the important ones with the following descriptions (the first argument to each ``test``):

    - 'log in as a new user, log out and log back in'
    - 'log in as three different users and start following each other'
    - 'error when following self'
    - 'error when adding a user that you are already following'

Before this, we need to set up the environment in which the tests run.
At the top of the file we have some global state that we use throughout.
Specifically, we have child processes for the ``daml start`` and ``npm start`` commands, which run for the duration of our tests.
We also have a single Puppeteer browser that we share among tests, opening new browser pages for each one.

The ``beforeAll()`` section is a function run once before any of the tests run.
We use it to spawn the ``daml start`` and ``npm start`` processes and launch the browser.
On the other hand the ``afterAll()`` section is used to shut down these processes and close the browser.
This step is important to prevent child processes persisting in the background after our program has finished.


Example: Log In and Out
=======================

Now let's get to a test!
The idea is to control the browser in the same way we would expect a user to in each scenario we want to test.
This means we use Puppeteer to type text into input forms, click buttons and search for particular elements on the page.
In order to find those elements, we do need to make some adjustments in our React components, which we'll show later.
Let's start at a higher level with a ``test``.

.. literalinclude:: code/testing/index.test.ts
  :language: ts
  :start-after: // LOGIN_TEST_BEGIN
  :end-before: // LOGIN_TEST_END

We'll walk though this step by step.

    - The ``test`` syntax is provided by Jest to indicate a new test running the function given as an argument (along with a description and time limit).
    - ``getParty()`` gives us a new party name. Right now it is just a string unique to this set of tests, but in the future we will use the Party Management Service to allocate parties.
    - ``newUiPage()`` is a helper function that uses the Puppeteer browser to open a new page (we use one page per party in these tests), navigate to the app URL and return a ``Page`` object.
    - Next we ``login()`` using the new page and party name. This should take the user to the main screen. We'll show how the ``login()`` function does this shortly.
    - We use the ``@daml/ledger`` library to check the ledger state. In this case, we want to ensure there is a single ``User`` contract created for the new party. Hence we create a new connection to the ``Ledger``, ``query()`` it and state what we ``expect`` of the result. When we run the tests, Jest will check these expectations and report any failures for us to fix.
    - The test also simulates the new user logging out and then logging back in. We again check the state of the ledger and see that it's the same as before.
    - Finally we must ``close()`` the browser page, which was opened in ``newUiPage()``, to avoid runaway Puppeteer processes after the tests finish.

You will likely use ``test``, ``getParty()``, ``newUiPage()`` and ``Browser.close()`` for all your tests.
In this case we use the ``@daml/ledger`` library to inspect the state of the ledger, but usually we just check the contents of the web page match our expectations.


Accessing UI Elements
=====================

We showed how to write a simple test at a high level, but haven't shown how to make individual actions in the app using Puppeteer.
This was hidden in the ``login()`` and ``logout()`` functions.
Let's see how ``login()`` is implemented.

.. literalinclude:: code/testing/index.test.ts
  :language: ts
  :start-after: // LOGIN_FUNCTION_BEGIN
  :end-before: // LOGIN_FUNCTION_END

We first wait to receive a handle to the username input element.
This is important to ensure the page and relevant elements are loaded by the time we try to act on them.
We then use the element handle to click into the input and type the party name.
Next we click the login button (this time assuming the button has loaded along with the rest of the page).
Finally, we wait until we find we've reached the menu on the main page.

The strings used to find UI elements, e.g. ``'.test-select-username-field'`` and ``'.test-select-login-button'``, are `CSS Selectors <https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Selectors>`_.
You may have seen them before in CSS styling of web pages.
In this case we use *class selectors*, which look for CSS classes we've given to elements in our React components.

This means we must manually add classes to the components we want to test.
For example, here is a snippet of the ``LoginScreen`` React component with classes added to the ``Form`` elements.

.. literalinclude:: code/templates-tarball/create-daml-app/ui/src/components/LoginScreen.tsx
  :language: tsx
  :start-after: {/* FORM_BEGIN */}
  :end-before: {/* FORM_END */}

You can see the ``className`` attributes in the ``Input`` and ``Button``, which we select in the ``login()`` function.
Note that you can use other features of an element in your selector, such as its type and attributes.
We've only used class selectors in these tests.


Writing CSS Selectors
=====================

When writing CSS selectors for your tests, you will likely need to check the structure of the rendered HTML in your app by running it manually and inspecting elements using your browser's developer tools.
For example, the image below is from inspecting the username field using the developer tools in Google Chrome.

   .. figure:: images/inspect-element.png
      :alt: The app login screen and the code that renders as that screen. In the code a line that reads "<div class = "field test select username field"> == $0" is highlighted.

There is a subtlety to explain here due to the `Semantic UI <https://semantic-ui.com/>`_ framework we use for our app.
Semantic UI provides a convenient set of UI elements which get translated to HTML.
In the example of the username field above, the original Semantic UI ``Input`` is translated to nested ``div`` nodes with the ``input`` inside.
You can see this highlighted on the right side of the screenshot.
While harmless in this case, in general you may need to inspect the HTML translation of UI elements and write your CSS selectors accordingly.


.. _Full Test Suite:

The Full Test Suite
===================

.. literalinclude:: code/testing/index.test.ts
  :language: ts
