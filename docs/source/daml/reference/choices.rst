.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: choices
##################

This page gives reference information on choices:

.. contents:: :local:

For the structure of a choice, see :doc:`structure`.

.. _daml-ref-controllers:

Controllers
***********

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :lines: 22
   :dedent: 4

- ``controller`` keyword
- The controller is a comma-separated list of values, where each value is either a party or a collection of parties.

  The conjunction of **all** the parties are required to authorize when this choice is exercised.

.. _daml-ref-choice-name:

Choice name
***********

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :lines: 22,24
   :dedent: 4

- The name of the choice. Must begin with a capital letter.
- Must be unique in your project. Choices in different templates can't have the same name.
- You can have multiple choices after one ``can``, for tidiness.

.. _daml-ref-anytime:

Non-consuming choices
*********************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :lines: 22,29
   :dedent: 4

- ``nonconsuming`` keyword. Optional.
- Makes a choice non-consuming: that is, exercising the choice does not archive the contract.

  By default, choices are *consuming*: when a choice on a contract is exercised, that contract instance is *archived*. Archived means that it's permanently marked as being inactive, and no more choices can be exercised on it, though it still exists on the ledger.
- This is useful in the many situations when you want to be able to exercise a choice more than once.

.. _daml-ref-return-type:

Return type
***********

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :lines: 22,42
   :dedent: 4

- Return type is written immediately after choice name.
- All choices have a return type. A contract returning nothing should be marked as returning a "unit", ie ``()``.
- If a contract is/contracts are created in the choice body, usually you would return the contract ID(s) (which have the type ``ContractId <name of template>``). This is returned when the choice is exercised, and can be used in a variety of ways.

.. _daml-ref-choice-arguments:

Choice arguments
****************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :lines: 22,35-37
   :dedent: 4

- ``with`` keyword.
- Choice arguments are similar in structure to :ref:`daml-ref-template-parameters`: a :ref:`record type <daml-ref-record-types>`.
- A choice argument can't have the same name as any :ref:`parameter to the template <daml-ref-template-parameters>` the choice is in.
- Optional - only if you need extra information passed in to exercise the choice.

.. _daml-ref-choice-body:

Choice body
***********

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :lines: 22,42-44

- Introduced with ``do``
- The logic in this section is what is executed when the choice gets exercised.
- The choice body contains ``Update`` expressions. For detail on this, see :doc:`updates`.
- By default, the last expression in the choice is returned. You can return multiple updates in tuple form or in a custom data type. To return something that isn't of type ``Update``, use the ``return`` keyword. 
