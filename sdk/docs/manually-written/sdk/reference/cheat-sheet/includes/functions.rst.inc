Functions
---------

.. list-table::

   * - Signature
     - ``f : Text -> Text -> Text``
   * - Definition
     - ``f x y = x <> " " <> y``
   * - Lambda definition
     - ``\x y -> x <> y``
   * - Polymorphic functions
     - ``f : (Show a, Eq a) => a -> Text -> Text``
   * - Function application
     - ``f "hello" "world!"``

Partial application of functions

.. code-block::

   salute : Text -> Text
   salute = f "Hello"


Functions are first class members of Daml, in particular, functions can be arguments to functions

.. code-block::

   apply : (Text -> Text) -> Text -> Text
   apply h x = h x

   apply salute "John" -- "Hello John"
