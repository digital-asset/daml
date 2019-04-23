.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: templates
####################

This page gives reference information on templates:

.. contents:: :local:

For the structure of a template, see :doc:`structure`.

.. _daml-ref-template-name:

Template name
*************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start template intro snippet
   :end-before: -- end template intro snippet

- This is the name of the template. It's preceded by ``template`` keyword. Must begin with a capital letter.
- This is the highest level of nesting.
- The name is used when :ref:`creating <daml-ref-create>` a contract instance of this template (usually, from within a choice).

.. _daml-ref-template-parameters:

Template parameters
*******************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start template params snippet
   :end-before: -- end template params snippet

- ``with`` keyword. The parameters are in the form of a :ref:`record type <daml-ref-record-types>`.
- Passed in when :ref:`creating <daml-ref-create>` a contract instance from this template. These are then in scope inside the template body.
- A template parameter can't have the same name as any :ref:`choice arguments <daml-ref-choice-arguments>` inside the template.

- You must pass in the parties as parameters to the contract.

  This means isn't valid to replace the ``giver`` variable in the ``Payout`` template above directly with ``'Elizabeth'``.

  Parties can only be named explicitly in scenarios.

.. Template has an *associated* data type with the same name?

.. _daml-ref-signatories:

Signatory parties
*****************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start template sigs snippet
   :end-before: -- end template sigs snippet

- ``signatory`` keyword. After ``where``. Followed by at least one ``Party``.
- Signatories are the parties (see the ``Party`` type) who must consent to the creation of an instance of this contract. They are the parties who would be put into an *obligable position* when this contract is created.

  DAML won't let you put someone into an obligable position without their consent. So if the contract will cause obligations for a party, they *must* be a signatory.
- When a signatory consents to the contract creation, this means they also authorize the consequences of :ref:`choices <daml-ref-choices>` that can be exercised on this contract.
- The contract instance is visible to all signatories (as well as the other stakeholders of the contract).
- You must have least one signatory per template. You can have many, either as a comma-separated list or reusing the keyword.

.. _daml-ref-observers:

Observers
*********

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start template obs snippet
   :end-before: -- end template obs snippet

- ``observer`` keyword. After ``where``. Followed by at least one ``Party``.
- Observers are additional stakeholders, so the contract instance is visible to these parties (see the ``Party`` type).
- Optional. You can have many, either as a comma-separated list or reusing the keyword.
- Use when a party needs visibility on a contract, or be informed or contract events, but is not a :ref:`signatory <daml-ref-signatories>` or :ref:`controller <daml-ref-controllers>`.
- TODO observer observers
- TODO flexible controllers vs automatic adding if other option

.. _daml-ref-choices:

Choices
*******

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start template choice snippet
   :end-before: -- start template choice snippet

- A right that the contract gives the controlling party. Can be *exercised*.
- This is essentially where all the logic of the template goes.
- By default, choices are *consuming*: that is, exercising the choice archives the contract, so no further choices can be exercised on it. You can make a choice non-consuming using the ``nonconsuming`` keyword.
- TODO: Two different ways of specifying - choice first or controller first
- See :doc:`choices` for full reference information.

.. _daml-ref-agreements:

Agreements
**********

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start template agree snippet
   :end-before: -- end template agree snippet

- ``agreement`` keyword, followed by text.
- Represents what the contract means in text. They're usually the boundary between on-ledger and off-ledger rights and obligations.
- Usually, they look like ``agreement tx``, where ``tx`` is of type ``Text``.

  You can use the built-in operator ``show`` to convert party names to a string, and concatenate with ``<>`` .

.. _daml-ref-preconditions:

Preconditions
*************

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :start-after: -- start template ensure snippet
   :end-before: -- end template ensure snippet

- ``ensure`` keyword, followed by a boolean condition.
- Used on contract creation. ``ensure`` limits the values on parameters that can be passed to the contract: the contract can only be created if the boolean condition is true.

