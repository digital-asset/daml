.. Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Interfaces
==========

This section contains a step-by-step tutorial that explains what interfaces are,
and how they can be used to provide a stable public API for contracts.

This tutorial should be of particular interest if you would like to learn how `CIP-0056`_ (the Canton Network Token Standard) works.

.. hint::

  Remember that you can load all the code for this section into a folder called ``intro-interfaces`` by running ``dpm new intro-interfaces  --template daml-intro-interfaces``

Community library
-----------------

This example uses the theme of a community library.
Library members can donate new items to the library catalog, and borrow items in the catalog.

The members of this library currently have books and discs they want to donate.
These are clearly different things and each have unique fields (e.g. books have authors, discs have formats).

However, this is not an exhaustive list: in the future, new members may want to donate entirely new types of items, for example magazines.
We would like to be able to do that without upgrading the Daml code of the library itself.

This is possible using interfaces: these let us define a stable API (items that can be loaned),
while allowing for several implementations that can be independently changed.

Project structure
-----------------

In :ref:`project-structures` you learned that it is important to keep the scripts and templates in separate packages.

This is even more important when dealing with interfaces:
packages containing interfaces can never be upgraded.

By placing the interface in a dedicated package,
and using dedicated packages for the contract templates implementing the interfaces,
we will be able to upgrade the latter.

We end up with the following packages:

``catalog-item``
    Defines the ``CatalogItem`` interface, the API for items we can borrow from the library

``book``
    Defines a ``Book`` contract, and its ``CatalogItem`` implementation

``disc``
    Defines a ``Disc`` contract, and its ``CatalogItem`` implementation

``library``
    Defines a community library in terms of ``CatalogItem``, so it can be
    extended with new catalog items (e.g. magazines) without affecting this
    package

``library-tests``
    Contains some basic Daml Script tests to show how the library can be used

Remember that you can ``dpm build --all`` at the root of the multi-package project to build all sub-packages.

Two contract templates
----------------------

First, let's take a look at the contract templates underpinning our library.
Note that these live in separate packages.
In a real-world use case, you can imagine that these packages are authored by different institutions.

We have a contract template for books:

.. literalinclude:: daml/daml-intro-interfaces/book/daml/Library/Book.daml
  :language: daml
  :start-after: -- BOOK_CONTRACT_BEGIN
  :end-before: -- BOOK_CONTRACT_END

And we have a contract template for discs:

.. literalinclude:: daml/daml-intro-interfaces/disc/daml/Library/Disc.daml
  :language: daml
  :start-after: -- DISC_CONTRACT_BEGIN
  :end-before: -- DISC_CONTRACT_END

Note how discs has a slightly different set of fields:

- We have different info: ``format`` rather than an ``author``
- A different naming convention is used for borrowing: ``borrowedBy`` rather than ``loanedTo``

Mismatches like this are exceedingly common when looking at real-world assets,
and prevent us from writing generic transactions dealing with these.

By defining an ``interface`` and getting the developers of the ``book`` and ``disc`` packages to align on this, we can fix that problem.

Defining the interface
----------------------

The first step to defining the interface is to create a Daml record to serve as the "interface view" (or simply "view").

The view includes fields for values that we require for both books and discs (and future catalog items!).

.. literalinclude:: daml/daml-intro-interfaces/catalog-item/daml/Library/CatalogItem.daml
  :language: daml
  :start-after: -- CATALOG_ITEM_VIEW_BEGIN
  :end-before: -- CATALOG_ITEM_VIEW_END

This interface view also defines the shape of the data that is returned when querying the ledger API or PQS for "all catalog items".
For example, in the ledger API you can use ``InterfaceFilter`` in ``filtersByParty``.

Next, we define the interface type, and we tie it to the ``CatalogItemView``:

.. literalinclude:: daml/daml-intro-interfaces/catalog-item/daml/Library/CatalogItem.daml
  :language: daml
  :start-after: -- CATALOG_ITEM_HEAD_BEGIN
  :end-before: -- CATALOG_ITEM_HEAD_END

Aside from the view, an interface can define a number of functions.
These must be implemented by the contract templates that want to implement this interface.
You can think of these as "abstract functions" in Object-Oriented-Programming terminology.

In our simple example, we want to be able to do three things with catalog items:

1. Disclose them, so other library members can see them;
2. Borrow them, and of course;
3. Return them again.

.. literalinclude:: daml/daml-intro-interfaces/catalog-item/daml/Library/CatalogItem.daml
  :language: daml
  :start-after: -- CATALOG_ITEM_METHODS_BEGIN
  :end-before: -- CATALOG_ITEM_METHODS_END

Finally, we can also define a number of choices on interfaces.
These generally work the same way as choices on regular contract templates.

However, there are a two important things to notice:

- In the implementation for this choices, we are able to use the functions we previously associated with the interface.
  This will allow us to exercise these choices on `all catalog items`, current and future.

- The choices have access to a function named `view`, which returns an instance of the interface view type.
  In our case, this is ``CatalogItemView``.
  This allows us to access the required fields from the view type.
  This is necessary since we don't know what fields the concrete contract type has (it could be a book, a disc, or something else entirely).

.. literalinclude:: daml/daml-intro-interfaces/catalog-item/daml/Library/CatalogItem.daml
  :language: daml
  :start-after: -- CATALOG_ITEM_CHOICES_BEGIN
  :end-before: -- CATALOG_ITEM_CHOICES_END

Implementing the interface
--------------------------

Now that we have defined the interface,
the respective authors of the ``book`` and ``disc`` packages can decide to implement it,
such that their assets can be used in the community library.

Implementing an interface concretely means:

1.  Providing a conversion function from the concrete contract to the interface view
2.  Implementing the functions defined in the interface

This implementation is added to the concrete contract templates in the Daml code.

First we have the contract template, as we already saw before:

.. literalinclude:: daml/daml-intro-interfaces/book/daml/Library/Book.daml
  :language: daml
  :start-after: -- BOOK_CONTRACT_BEGIN
  :end-before: -- BOOK_CONTRACT_END

And then we can place the interface implementation immediately after, using the keywords ``interface instance``:

.. literalinclude:: daml/daml-intro-interfaces/book/daml/Library/Book.daml
  :language: daml
  :start-after: -- BOOK_INTERFACE_INSTANCE_BEGIN
  :end-before: -- BOOK_INTERFACE_INSTANCE_END

Implementing the `view` is easy: we just need to construct a ``CatalogItemView`` based on our concrete ``Book``.

In a sense, it is analogous to using ``CREATE VIEW`` in SQL databases to create consistent interfaces, which is considered a good database design pattern.

Implementing the required functions is similarly straightforward.
If we make the comparison to Object-Oriented-Programming again, this would be implementing the abstract methods of an abstract class or interface.

One thing you will notice is that we use the function ``toInterfaceContractId`` from the standard library.
This is required to implement the methods correctly.
Our interface defined that these functions must return ``ConstractId CatalogItem``,
and just returning the result of ``create this`` would give us a ``ContractId Book`` instead.
``toInterfaceContractId`` can safely convert contract IDs of a concrete contract to a contract ID of an interface implemented by that concrete contract template.

Using the interface
-------------------

We can now write stable Daml code against the ``CatalogItem`` interface, without relying on specific versions of the ``book`` and ``disc`` packages.

For the purpose of this tutorial, we will continue to do this in Daml, by writing an on-ledger model for the community library.
But bear in mind that you can also jump straight to the Ledger API from here,
and use that to, for example: list all catalog item contracts,
read from their interface view,
or exercise choices defined on the interface.

In Daml, we can use these interfaces in the same way as we use regular contract templates.

In this abbreviated snippet, we show only one choice on the ``Library``, but you can find the full version in the directory you created for this tutorial.

.. literalinclude:: daml/daml-intro-interfaces/library/daml/Library.daml
  :language: daml
  :start-after: -- LIBRARY_HEAD_BEGIN
  :end-before: -- LIBRARY_HEAD_END

In the ``Library_Donate`` choice, we use ``view``, just as we did in the choices we defined in the interface earlier, to obtain the interface view.
This then lets us access the fields of that view, in particular ``owner``.

Conclusion
----------

With that, we can rest assured our library contract will be able to deal with any current and future catalog items,
and package publishers can add arbitrary catalog items by implementing our interface.

At the end of this tutorial, you may find it disappointing that this community library does not actually exist.
However, the mechanisms explained here underpin a lot of the important activity on the Canton network.

In particular, `CIP-0056`_, the Canton Network Token Standard, defines tokens using a Daml interface.
This allows application implementers to interact with various assets, such as CC or USDC,
using a well-defined API that is future- and backwards-compatible.

.. _CIP-0056: https://github.com/canton-foundation/cips/blob/main/cip-0056/cip-0056.md
