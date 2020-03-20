.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Testing Your App
****************

When developing your application, you will want to test certain workflows through the UI.
In particular, you'll want to make sure that changes such as new features do not break existing functionality by mistake.
It may be fine to do this testing manually at first, but it quickly becomes more tiresome and error prone as the app grows.

In this section we will show you one approach to automatically testing workflows in your UI.
This will allow you to iterate on your app faster and with more confidence that you aren't breaking it!

Testing with Jest and Puppeteer
===============================

There are two tools that we chose to write end to end tests for our app.
Of course there are more to choose from but we will show you one path here.
The first tool is a general JavaScript testing framework called
`Jest <https://jestjs.io/>`_.
Jest allows you to write test cases containing expectations about behaviour should occur.
If the expectations for a test are not met, then you should see helpful messages about what went wrong.
You can also write setup and cleanup procedures that run before and after each test.
This will be very useful for us to manage services like the DAML sandbox and JSON API server.

The second tool we use heavily is a library called
`Puppeteer <https://pptr.dev/>`_.
Puppeteer allows you to control a Chrome browser from a NodeJS program.
In our case we use it to perform actions like logging in, following friends and sending messages.
We can also control multiple browser pages at once and test the interactions between different users.
As a bonus, if something goes wrong you can take a screenshot and see what state your test browser got into!

Let's see how to use these tools to write some tests for our social network app.
You can see the full suite in the file ``index.test.tsx``.
We first have some global state that we will use throughout our tests.
Specifically, we have child processes for the ``daml start`` and ``yarn start`` commands, which run for the duration of our tests.
We also have a single Puppeteer browser that we share among tests, opening new pages in each.

The ``beforeAll()`` section is a function run once before any of the tests run.
We use it to spawn the ``daml start`` and ``yarn start`` processes and launch the browser.
On the other hand the ``afterAll()`` section is used to shut down these processes and close the browser.
This step is important to prevent child processes persisting in the background after our program has finished.

Now let's get to the tests!
The idea is to control the browser in the same way we would expect a user to in each scenario we want to test.
This means we use Puppeteer to type text into input forms, click buttons and search for particular elements on the page.
In order to find those elements, we do need to make some adjustments in our React components, which we'll show later.
Let's start at a higher level with a ``test``.

.. literalinclude:: code/ui-before/index.test.tsx
  :language: tsx
  :start-after: // BEGIN_LOGIN_TEST
  :end-before: // END_LOGIN_TEST

We'll walk though this step by step.

    - The ``test`` syntax is provided by Jest to indicate a new test running the function given as an argument (along with a description and time limit).
    - ``getParty()`` gives us a new party name. Right now it is just a string unique to this set of tests, but in the future we will use the Party Management Service to allocate parties.
    - ``newUiPage()`` is a helper function that uses the Puppeteer browser to open a new page (we use one page per party in these tests), navigate to the app URL and return a ``Page`` object.
    - Next we ``login()`` using the new page and party name. This should take the user to the main screen. We'll show how the ``login()`` function does this shortly.
    - We use the ``@daml/ledger`` library to check the ledger state. In this case, we want to ensure there is a single ``User`` contract created for the new party. Hence we create a new connection to the ``Ledger``, ``query()`` it and state what we ``expect`` of the result. When we run the tests, Jest will check these expectations and report any failures for us to fix.
    - The test also simulates the new user logging out and then logging back in. We again check the state of the ledger and see that it's the same as before.
    - Finally we must ``close()`` the browser page, which was opened in ``newUiPage()``, to avoid runaway Puppeteer processes after the tests finish.

Selecting and interacting with UI elements
==========================================

We showed how to write a simple test at a high level, but haven't shown how to make individual actions in the app using Puppeteer.
This was hidden in the ``login()`` and ``logout()`` functions.
Let's see how ``login()`` is implemented.

.. literalinclude:: code/ui-before/index.test.tsx
  :language: tsx
  :start-after: // BEGIN_LOGIN_FUNCTION
  :end-before: // END_LOGIN_FUNCTION

This looks somewhat mysterious at first.
We first get some sort of handle to the username input element, and then use it to click into it and type the party name.
Then we directly click the login button.
Finally, we wait until we find we've reached the menu on the main page.

The strings used to find UI elements, e.g. ``'.test-select-username-field'`` and ``'.test-select-login-button'``, are `CSS Selectors <https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Selectors>`_.
You may have seen them before if you've done CSS styling of web pages.
In this case we use *class selectors*, which look for CSS classes we've given to elements in our React components.

Here is a snippet of the ``LoginScreen`` React component, showing the ``Form`` elements that have the added classes.

.. literalinclude:: code/ui-before/LoginScreen.tsx
  :language: tsx
  :start-after: // BEGIN_FORM
  :end-before: // END_FORM

You can see the ``className`` attributes in the ``Input`` and ``Button`` which we select in the ``login()`` function.
Note that you can use other features of the elements to select, such as their types and attributes.
We just show simple class selectors here.

When writing CSS selectors for your tests, you will likely need to check the structure of the rendered HTML in your app by running it manually and inspecting elements using your browser's developer tools.
For example, the image below is from inspecting the username field using Google Chrome's developer tools.

   .. figure:: images/inspect-element.png

There is a subtlety to explain here due to the `Semantic UI <https://semantic-ui.com/>`_ framework we use for our app.
Semantic UI provides a convenient set of UI elements which get translated to HTML.
In the example of the username field above, the original Semantic UI ``Input`` is translated to nested ``div`` nodes with the ``input`` inside.
While harmless in this case, the translation process means you may need to tweak your CSS selectors to access the intended elements in the generated HTML.
