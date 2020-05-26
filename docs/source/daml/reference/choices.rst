.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: choices
##################

This page gives reference information on choices:

.. contents:: :local:

For information on the high-level structure of a choice, see :doc:`structure`.

``choice`` first or ``controller`` first
****************************************

There are two ways you can start a choice:

- start with the ``choice`` keyword
- start with the ``controller`` keyword

.. literalinclude:: ../code-snippets/Structure.daml
   :language: daml
   :start-after: -- start of choice snippet
   :end-before: -- end of choice snippet

The main difference is that starting with ``choice`` means that you can pass in a ``Party`` to use as a controller. If you do this, you **must** make sure that you add that party as an ``observer``, otherwise they won't be able to see the contract (and therefore won't be able to exercise the choice).

In contrast, if you start with ``controller``, the ``controller`` is automatically added as an observer when you compile your DAML files.

.. _daml-ref-choice-name:

Choice name
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
   :caption: Option 2 for specifying choices: controller first


- The name of the choice. Must begin with a capital letter.
- If you're using choice-first, preface with ``choice``. Otherwise, this isn't needed.
- Must be unique in your project. Choices in different templates can't have the same name.
- If you're using controller-first, you can have multiple choices after one ``can``, for tidiness.

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
   :caption: Option 2 for specifying choices: controller first

- ``controller`` keyword
- The controller is a comma-separated list of values, where each value is either a party or a collection of parties.

  The conjunction of **all** the parties are required to authorize when this choice is exercised.

.. _daml-ref-consumability:

Contract consumption
====================

If no qualifier is present, choices are *consuming*: the contract is archived before the evaluation of the choice body and both the controllers and all contract stakeholders see all consequences of the action.

Preconsuming choices
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
   :caption: Option 2 for specifying choices: controller first

- ``preconsuming`` keyword. Optional.
- Makes a choice pre-consuming: the contract is archived before the body of the exercise is executed.
- The archival behavior is analogous to the *consuming* default behavior.
- Unlike what happens the in *consuming* behavior, though, only the controllers and signatories of the contract see all consequences of the action. If the choice archives the contract, other stakeholders merely see an archive action.
- Can be thought as a non-consuming choice that implicitly archives the contract before anything else happens

Postconsuming choices
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
   :caption: Option 2 for specifying choices: controller first

- ``postconsuming`` keyword. Optional.
- Makes a choice post-consuming: the contract is archived after the body of the exercise is executed.
- The contract can still be used in the body of the exercise.
- Only the controllers and signatories of the contract see all consequences of the action. If the choice archives the contract, other stakeholders merely see an archive action.
- Can be thought as a non-consuming choice that implicitly archives the contract after the choice has been exercised

Non-consuming choices
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
   :caption: Option 2 for specifying choices: controller first

- ``nonconsuming`` keyword. Optional.
- Makes a choice non-consuming: that is, exercising the choice does not archive the contract.
- Only the controllers and signatories of the contract see all consequences of the action. If the choice archives the contract, other stakeholders merely see an archive action.
- Useful in the many situations when you want to be able to exercise a choice more than once.

.. _daml-ref-return-type:

Return type
===========

- Return type is written immediately after choice name.
- All choices have a return type. A contract returning nothing should be marked as returning a "unit", ie ``()``.
- If a contract is/contracts are created in the choice body, usually you would return the contract ID(s) (which have the type ``ContractId <name of template>``). This is returned when the choice is exercised, and can be used in a variety of ways.

.. _daml-ref-choice-arguments:

Choice arguments
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

Choice body
***********

- Introduced with ``do``
- The logic in this section is what is executed when the choice gets exercised.
- The choice body contains ``Update`` expressions. For detail on this, see :doc:`updates`.
- By default, the last expression in the choice is returned. You can return multiple updates in tuple form or in a custom data type. To return something that isn't of type ``Update``, use the ``return`` keyword. 
