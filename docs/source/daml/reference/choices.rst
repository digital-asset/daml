.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: Choices
##################

This page gives reference information on choices. For information on the high-level structure of a choice, see :doc:`structure`.

``choice`` First or ``controller`` First
****************************************

There are two ways you can start a choice:

- start with the ``choice`` keyword
- start with the ``controller`` keyword

.. warning::

  ``controller`` first syntax is deprecated since Daml 2.0 and will be removed in a future version. For more information, see :ref:`daml-ref-controller-can-deprecation`.

.. literalinclude:: ../code-snippets/Structure.daml
   :language: daml
   :start-after: -- start of choice snippet
   :end-before: -- end of choice snippet

The main difference is that starting with ``choice`` means that you can pass in a ``Party`` to use as a controller. If you do this, you **must** make sure that you add that party as an ``observer``, otherwise they won't be able to see the contract (and therefore won't be able to exercise the choice).

In contrast, if you start with ``controller``, the ``controller`` is automatically added as an observer when you compile your Daml files.

.. _daml-ref-choice-observers:

A secondary difference is that starting with ``choice`` allows *choice observers* to be attached to the choice using the ``observer`` keyword. The choice observers are a list of parties that, in addition to the stakeholders, will see all consequences of the action.

.. literalinclude:: ../code-snippets/Structure.daml
   :language: daml
   :start-after: -- start of choice observer snippet
   :end-before: -- end of choice observer snippet

.. _daml-ref-choice-name:

Choice Name
***********

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start choice-first choice name snippet
   :end-before: -- end choice-first choice name snippet
   :caption: Option 1 for specifying choices: choice name first

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start controller-first choice name snippet
   :end-before: -- end controller-first choice name snippet
   :caption: Option 2 for specifying choices (deprecated syntax): controller first


- The name of the choice. Must begin with a capital letter.
- If you're using choice-first, preface with ``choice``. Otherwise, this isn't needed.
- Must be unique in your project. Choices in different templates can't have the same name.
- If you're using controller-first, you can have multiple choices after one ``can``, for tidiness. However, note that this syntax is deprecated and will be removed in a future version of Daml.

.. _daml-ref-controllers:

Controllers
***********

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start choice-first controller snippet
   :end-before: -- end choice-first controller snippet
   :caption: Option 1 for specifying choices: choice name first

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start controller-first controller snippet
   :end-before: -- end controller-first controller snippet
   :caption: Option 2 for specifying choices (deprecated syntax): controller first

- ``controller`` keyword
- The controller is a comma-separated list of values, where each value is either a party or a collection of parties.

  The conjunction of **all** the parties are required to authorize when this choice is exercised.

.. _daml-ref-consumability:

Contract Consumption
====================

If no qualifier is present, choices are *consuming*: the contract is archived before the evaluation of the choice body and both the controllers and all contract stakeholders see all consequences of the action.

Preconsuming Choices
********************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start choice-first preconsuming snippet
   :end-before: -- end choice-first preconsuming snippet
   :caption: Option 1 for specifying choices: choice name first

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start controller-first preconsuming snippet
   :end-before: -- end controller-first preconsuming snippet
   :caption: Option 2 for specifying choices (deprecated syntax): controller first

- ``preconsuming`` keyword. Optional.
- Makes a choice pre-consuming: the contract is archived before the body of the exercise is executed.
- The create arguments of the contract can still be used in the body of the exercise, but cannot be fetched by its contract id.
- The archival behavior is analogous to the *consuming* default behavior.
- Only the controllers and signatories of the contract see all consequences of the action. Other stakeholders merely see an archive action.
- Can be thought as a non-consuming choice that implicitly archives the contract before anything else happens

Postconsuming Choices
*********************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start choice-first postconsuming snippet
   :end-before: -- end choice-first postconsuming snippet
   :caption: Option 1 for specifying choices: choice name first

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start controller-first postconsuming snippet
   :end-before: -- end controller-first postconsuming snippet
   :caption: Option 2 for specifying choices (deprecated syntax): controller first

- ``postconsuming`` keyword. Optional.
- Makes a choice post-consuming: the contract is archived after the body of the exercise is executed.
- The create arguments of the contract can still be used in the body of the exercise as well as the contract id for fetching it.
- Only the controllers and signatories of the contract see all consequences of the action. Other stakeholders merely see an archive action.
- Can be thought as a non-consuming choice that implicitly archives the contract after the choice has been exercised

Non-consuming Choices
*********************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start choice-first nonconsuming snippet
   :end-before: -- end choice-first nonconsuming snippet
   :caption: Option 1 for specifying choices: choice name first

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start controller-first nonconsuming snippet
   :end-before: -- end controller-first nonconsuming snippet
   :caption: Option 2 for specifying choices (deprecated syntax): controller first

- ``nonconsuming`` keyword. Optional.
- Makes a choice non-consuming: that is, exercising the choice does not archive the contract.
- Only the controllers and signatories of the contract see all consequences of the action.
- Useful in the many situations when you want to be able to exercise a choice more than once.

.. _daml-ref-return-type:

Return Type
===========

- Return type is written immediately after choice name.
- All choices have a return type. A contract returning nothing should be marked as returning a "unit", ie ``()``.
- If a contract is/contracts are created in the choice body, usually you would return the contract ID(s) (which have the type ``ContractId <name of template>``). This is returned when the choice is exercised, and can be used in a variety of ways.

.. _daml-ref-choice-arguments:

Choice Arguments
****************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start choice-first params snippet
   :end-before: -- end choice-first params snippet

- ``with`` keyword.
- Choice arguments are similar in structure to :ref:`daml-ref-template-parameters`: a :ref:`record type <daml-ref-record-types>`.
- A choice argument can't have the same name as any :ref:`parameter to the template <daml-ref-template-parameters>` the choice is in.
- Optional - only if you need extra information passed in to exercise the choice.

.. _daml-ref-choice-body:

Choice Body
***********

- Introduced with ``do``
- The logic in this section is what is executed when the choice gets exercised.
- The choice body contains ``Update`` expressions. For detail on this, see :doc:`updates`.
- By default, the last expression in the choice is returned. You can return multiple updates in tuple form or in a custom data type. To return something that isn't of type ``Update``, use the ``return`` keyword.

.. _daml-ref-controller-can-deprecation:

Deprecation of ``controller`` first syntax
******************************************

Since Daml 2.0, using ``controller`` first syntax to define a choice will
result in the following warning:

.. code-block:: text

  The syntax 'controller ... can' is deprecated,
  it will be removed in a future version of Daml.
  Instead, use 'choice ... with ... controller' syntax.
  Note that 'choice ... with ... controller' syntax does not
  implicitly add the controller as an observer,
  so it must be added explicitly as one (or as a signatory).

Migrating
=========

Users are strongly encouraged to rewrite their choices using ``choice``
first syntax. This is a schema for such a rewrite:

#. For each ``controller ... can`` block,

   #. Note the parties between the ``controller`` and ``can`` keywords; these are the block controllers.

   #. Ensure that all the block controllers are signatories or observers of the template. If any controller is neither a signatory nor observer of the template, add it as an observer.

   #. For each choice in the block,

      #. Prefix the choice name with the ``choice`` keyword, but keep any consumption qualifiers before ``choice``.

      #. Add a ``controller`` clause with the block controllers before the body of the choice (the ``do`` block) .

   #. Remove the ``controller ... can`` block header and adjust indentation as necessary.

Disabling the warning
=====================

This warning is controlled by the warning flag ``controller-can``, which means
that it can be disabled independently of other warnings. This is especially
useful for gradually migrating code that used this syntax.

To disable the warning within a Daml file, add the following line at the top of
the file:

.. code-block:: daml

  {-# OPTIONS_GHC -Wno-controller-can #-}

To disable it for an entire Daml project, add the following entry to the
``build-options`` field of the project's ``daml.yaml`` file

.. code-block:: yaml

  build-options:
  - --ghc-option=-Wno-controller-can

Within a project where the warning has been disabled as described above, it can
be reenabled for a single Daml file by adding the following line at the top of
the file:

.. code-block:: daml

  {-# OPTIONS_GHC -Wcontroller-can #-}
