.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Introduction to the Daml Standard Library
=========================================

In :doc:`3_Data` and :doc:`10_Functional101` you learned how to define your own data types and functions. But of course you don't have to implement everything from scratch. Daml comes with the Daml Standard Library, which contains types, functions and typeclasses that cover a large range of use-cases. In this chapter, you'll get an overview of the essentials, but also learn how to browse and search this library to find functions. Being proficient with the Standard Library will make you considerably more efficient writing Daml code. Specifically, this chapter covers:

- The Prelude
- Important types from the Standard Library, and associated functions and typeclasses
- Typeclasses
- Important typeclasses like ``Functor``, ``Foldable``, and ``Traversable``
- How to search the Standard Library

To go in depth on some of these topics, the literature referenced in :ref:`haskell-connection` covers them in much greater detail. The Standard Library typeclasses like ``Applicative``, ``Foldable``, ``Traversable``, ``Action`` (called ``Monad`` in Haskell), and many more, are the bread and butter of Haskell programmers.

.. note::

  There is a project template ``daml-intro-11`` for this chapter, but it only contains a single source file with the code snippets embedded in this section.

The Prelude
-----------

You've already used a lot of functions, types, and typeclasses without importing anything. Functions like ``create``, ``exercise``, and ``(==)``, types like ``[]``, ``(,)``, ``Optional``, and typeclasses like ``Eq``, ``Show``, and ``Ord``. These all come from the :doc:`Prelude </daml/stdlib/Prelude>`. The Prelude is module that gets implicitly imported into every other Daml module and contains both Daml specific machinery as well as the essentials needed to work with the inbuilt types and typeclasses.

Important Types From the Prelude
--------------------------------

In addition to the :ref:`native-types`, the Prelude defines a number of common types:

Lists
.....

You've already met lists. Lists have two constructors ``[]`` and ``x :: xs``, the latter of which is "prepend" in the sense that ``1 :: [2] == [1, 2]``. In fact ``[1,2]`` is just syntactical sugar for ``1 :: 2 :: []``. 

Tuples
......

In addition to the 2-tuple you have already seen, the Prelude contains definitions for tuples of size up to 15.
Tuples allow you to store mixed data in an ad-hoc fashion. Common use-cases are return values from functions
consisting of several pieces or passing around data in folds, as you saw in :ref:`folds`. 
An example of a relatively wide Tuple can be found in the test modules of the :doc:`8_Exceptions` project.
``Test.Intro.Asset.TradeSetup.tradeSetup`` returns the allocated parties and active contracts in a long tuple.
``Test.Intro.Asset.MultiTrade.testMultiTrade`` puts them back into scope using pattern matching:

.. literalinclude:: daml/daml-intro-9/daml/Test/Intro/Asset/TradeSetup.daml
  :language: daml
  :start-after: -- TUPLE_RETURN_BEGIN
  :end-before: -- TUPLE_RETURN_END

.. literalinclude:: daml/daml-intro-9/daml/Test/Intro/Asset/MultiTrade.daml
  :language: daml
  :start-after: -- TUPLE_USE_BEGIN
  :end-before: -- TUPLE_USE_END

Tuples, like lists have some syntactic magic. Both the types as well as the constructors for tuples are ``(,,,)`` where the number of commas determines the arity of the tuple. Type and data constructor can be applied with values inside the brackets, or outside, and partial application is possible:

.. literalinclude:: daml/daml-intro-11/daml/Main.daml
  :language: daml
  :start-after: -- TUPLE_DEMO_BEGIN
  :end-before: -- TUPLE_DEMO_END

.. note::

    While tuples of great lengths are available, it is often advisable to define custom records with named fields for complex structures or long-lived values. Overuse of tuples can harm code readability.

Optional
........

The ``Optional`` type represents a value that may be missing. It's the closest thing Daml has to a "nullable" value. ``Optional`` has two constructors: ``Some``, which takes a value, and ``None``, which doesn't take a value. In many languages one would write code like this:

.. code-block:: JavaScript

  lookupResult = lookupByKey(k);

  if( lookupResult == null) {
    // Do something
  } else {
    // Do something else
  }

In Daml the same thing would be expressed as:

.. literalinclude:: daml/daml-intro-11/daml/Main.daml
  :language: daml
  :start-after: -- OPT_BEGIN
  :end-before: -- OPT_END

Either
......

``Either`` is used in cases where a value should store one of two types. It has two constructors, ``Left`` and ``Right``, each of which take a value of one or the other of the two types. One typical use-case of ``Either`` is as an extended ``Optional`` where ``Right`` takes the role of ``Some`` and ``Left`` the role of ``None``, but with the ability to store an error value. ``Either Text``, for example behaves just like ``Optional``, except that values with constructor ``Left`` have a text associated to them.

.. note::

  As with tuples, it's easy to overuse ``Either`` and harm readability. Consider writing your own more explicit type instead. For example if you were returning ``South a`` vs ``North b`` using your own type over ``Either`` would make your code clearer.

Typeclasses
-----------

You've seen typeclasses in use all the way from :doc:`3_Data`. It's now time to look under the hood.

Typeclasses are declared using the ``class`` keyword:

.. literalinclude:: daml/daml-intro-11/daml/Main.daml
  :language: daml
  :start-after: -- CLASS_BEGIN
  :end-before: -- CLASS_END

This is akin to an interface declaration of an interface with a getter and setter for a quantity. To *implement* this interface, you need to define instances of this typeclass:

.. literalinclude:: daml/daml-intro-11/daml/Main.daml
  :language: daml
  :start-after: -- INSTANCE_BEGIN
  :end-before: -- INSTANCE_END

Typeclasses can have constraints like functions. For example: ``class Eq a => Ord a`` means "everything that is orderable can also be compared for equality". And that's almost all there's to it. 

Important Typeclasses From the Prelude
--------------------------------------

Eq
...

The ``Eq`` typeclass allows values of a type to be compared for (in)-equality. It makes available two function: ``==`` and ``/=``. Most data types from the Standard Library have an instance of ``Eq``. As you already learned in :doc:`3_Data`, you can let the compiler automatically derive instances of ``Eq`` for you using the ``deriving`` keyword.

Templates always have an ``Eq`` instance, and all types stored on a template need to have one.

Ord
...

The ``Ord`` typeclass allows values of a type to be compared for order. It makes available functions: ``<``, ``>``, ``<=``, and ``>=``. Most of the inbuilt data types have an instance of ``Ord``. Furthermore, types like ``List`` and ``Optional`` get an instance of ``Ord`` if the type they contain has one. You can let the compiler automatically derive instances of ``Ord`` for you using the ``deriving`` keyword.

Show
....

``Show`` indicates that a type can be serialized to ``Text``, ie "shown" in a shell. Its key function is ``show``, which takes a value and converts it to ``Text``. All inbuilt data types have an instance for ``Show`` and types like ``List`` and ``Optional`` get an instance if the type they contain has one. It also supports the ``deriving`` keyword.

Functor
.......

:ref:`Functors <class-ghc-base-functor-31205>` are the closest thing to "containers" that Daml has. Whenever you see a type with a single type parameter, you are probably looking at a ``Functor``: ``[a]``, ``Optional a``, ``Either Text a``, ``Update a``. Functors are things that can be mapped over and as such, the key function of ``Functor`` is ``fmap``, which does generically what the ``map`` function does for lists.

Other classic examples of Functors are Sets, Maps, Trees, etc.

Applicative Functor
...................

:ref:`Applicative Functors <class-da-internal-prelude-applicative-9257>` are a bit like Actions, which you met in :doc:`5_Restrictions`, except that you can't use the result of one action as the input to another action. The only important Applicative Functor that isn't an action in Daml is the ``Commands`` type submitted in a ``submit`` block in Daml Script. That's why in order to use ``do`` notation in Daml Script, you have to enable the ``ApplicativeDo`` language extension.

Actions
.......

:ref:`Actions <class-da-internal-prelude-action-68790>` were already covered in :doc:`5_Restrictions`. One way to think of them is as "recipes" for a value, which need to be "executed to get at that value. Actions are always Functors (and Applicative Functors). The intuition for that is simply that ``fmap f x`` is the recipe in ``x`` with the extra instruction to apply the pure function ``f`` to the result.

The really important Actions in Daml are ``Update`` and ``Script``, but there are many others, like ``[]``, ``Optional``, and ``Either a``.

Semigroups and Monoids
......................

:ref:`Semigroups and monoids <class-da-internal-prelude-semigroup-78998>` are about binary operations, but in practice, their important use is for ``Text`` and ``[]``, where they allow concatenation using the ``{<>}`` operator.

Additive and Multiplicative
...........................

:ref:`Additive and Multiplicative <class-ghc-num-additive-25881>` abstract out arithmetic operations, so that ``(+)``, ``(-)``, ``(*)``, and some other functions can be used uniformly between ``Decimal`` and ``Int``.

Important Modules in the Standard Library
-----------------------------------------

For almost all the types and typeclasses presented above, the Standard Library contains a module:

- :doc:`/daml/stdlib/DA-List` for Lists
- :doc:`/daml/stdlib/DA-Optional` for ``Optional``
- :doc:`/daml/stdlib/DA-Tuple` for Tuples
- :doc:`/daml/stdlib/DA-Either` for ``Either``
- :doc:`/daml/stdlib/DA-Functor` for Functors
- :doc:`/daml/stdlib/DA-Action` for Actions
- :doc:`/daml/stdlib/DA-Monoid` and :doc:`/daml/stdlib/DA-Semigroup` for Monoids and Semigroups
- :doc:`/daml/stdlib/DA-Text` for working with ``Text``
- :doc:`/daml/stdlib/DA-Time` for working with ``Time``
- :doc:`/daml/stdlib/DA-Date` for working with ``Date``

You get the idea, the names are fairly descriptive.

Other than the typeclasses defined in Prelude, there are two modules generalizing concepts you've already learned, which are worth knowing about: ``Foldable`` and ``Traversable``. In :ref:`looping` you learned all about folds and their Action equivalents. All the examples there were based on lists, but there are many other possible iterators. This is expressed in two additional typeclasses: :doc:`/daml/stdlib/DA-Traversable`, and :doc:`/daml/stdlib/DA-Foldable`. For more detail on these concepts, please refer to the literature in :ref:`haskell-connection`, or `https://wiki.haskell.org/Foldable_and_Traversable <https://wiki.haskell.org/Foldable_and_Traversable>`__.

Search the Standard Library
---------------------------

Being able to browse the Standard Library starting from :doc:`/daml/stdlib/index` is a start, and the module naming helps, but it's not an efficient process for finding out what a function you've encountered does, or even less so to find a function that does a thing you need to do.

Daml has it's own version of the `Hoogle <https://hoogle.haskell.org/>`__ search engine, which offers search both by name and by signature. It's fully integrated into the search bar on `https://docs.daml.com/ <https://docs.daml.com/>`__, but for those wanting a pure Standard Library search, it's also available on `<https://hoogle.daml.com>`__.

Search for Functions by Name
............................

Say you come across some functions you haven't seen before, like the ones in the ``ensure`` clause of the ``MultiTrade``.

.. literalinclude:: daml/daml-intro-9/daml/Intro/Asset/MultiTrade.daml
  :language: daml
  :start-after: -- ENSURE_BEGIN
  :end-before: -- ENSURE_END

You may be able to guess what ``not`` and ``null`` do, but try searching those names in the documentation search. Search results from the Standard Library will show on top. ``not``, for example, gives

.. code-block:: none

   not
    
    : Bool -> Bool

    Boolean “not”

Signature (including type constraints) and description usually give a pretty clear picture of what a function does.

Search for Functions by Signature
.................................

The other very common use case for the search is that you have some values that you want to do something with, but don't know the standard library function you need. On the ``MultiTrade`` template we have a list ``baseAssets``, and thanks to your ensure clause we know it's non-empty. In the original ``Trade`` we used ``baseAsset.owner`` as the signatory. How do you get the first element of this list to extract the ``owner`` without going through the motions of a complete pattern match using ``case``?

The trick is to think about the signature of the function that's needed, and then to search for that signature. In this case, we want a single distinguished element from a list so the signature should be ``[a] -> a``. If you search for that, you'll get a whole range of results, but again, Standard Library results are shown at the top.

Scanning the descriptions, ``head`` is the obvious choice, as used in the ``let`` of the ``MultiTrade`` template.

You may notice that in the search results you also get some hits that don't mention ``[]`` explicitly. For example:

.. code-block::none

  fold
    
    : (Foldable t, Monoid m) => t m -> m

    Combine the elements of a structure using a monoid.

The reason is that there is an instance for ``Foldable [a]``.

Let's try another search. Suppose you didn't want the first element, but the one at index ``n``. Remember that ``(!!)`` operator from :doc:`10_Functional101`? There are now two possible signatures we could search for:  ``[a] -> Int -> a`` and ``Int -> [a] -> a``. Try searching for both. You'll see that the search returns ``(!!)`` in both cases. You don't have to worry about the order of arguments.

Next Up
-------

There's little more to learn about writing Daml at this point that isn't best learned by practice and consulting reference material for both Daml and Haskell. To finish off this course, you'll learn a little more about your options for testing and interacting with Daml code in :doc:`12_Testing`, and about the operational semantics of some keywords and common associated failures.
