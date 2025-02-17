.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: Choices
##################

This page gives reference information on choices. For information on the high-level structure of a choice, see :doc:`structure`.

.. _daml-ref-choice-name:

Choice Name
***********

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start choice name snippet
   :end-before: -- end choice name snippet

- ``choice`` keyword
- The name of the choice. Must begin with a capital letter.
- Must be unique in the module. Different templates defined in the same module cannot share a choice name.

.. _daml-ref-controllers:

Controllers
***********

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start controller snippet
   :end-before: -- end controller snippet

- ``controller`` keyword
- The controller is a comma-separated list of values, where each value is either a party or a collection of parties.

  The conjunction of **all** the parties are required to authorize when this choice is exercised.

.. warning::

  You **must** make sure that the controller parties are observers (or signatories) of the contract, otherwise they cannot see the contract (and therefore cannot exercise the choice).

.. _daml-ref-choice-observers:

Choice Observers
****************

*Choice observers* can be attached to a choice using the ``observer`` keyword. The choice observers are a list of parties who are not stakeholders but who see all the consequences of the action.

.. literalinclude:: ../code-snippets/Structure.daml
   :language: daml
   :start-after: -- start of choice observer snippet
   :end-before: -- end of choice observer snippet

.. _daml-ref-consumability:

Contract Consumption
====================

If no qualifier is present, choices are *consuming*: the contract is archived before the evaluation of the choice body and both the controllers and all contract stakeholders see all consequences of the action.

Preconsuming Choices
********************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start preconsuming snippet
   :end-before: -- end preconsuming snippet

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
   :start-after: -- start postconsuming snippet
   :end-before: -- end postconsuming snippet

- ``postconsuming`` keyword. Optional.
- Makes a choice post-consuming: the contract is archived after the body of the exercise is executed.
- The create arguments of the contract can still be used in the body of the exercise as well as the contract id for fetching it.
- Only the controllers and signatories of the contract see all consequences of the action. Other stakeholders merely see an archive action.
- Can be thought as a non-consuming choice that implicitly archives the contract after the choice has been exercised

Non-consuming Choices
*********************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start nonconsuming snippet
   :end-before: -- end nonconsuming snippet

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
   :start-after: -- start choice params snippet
   :end-before: -- end choice params snippet

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
