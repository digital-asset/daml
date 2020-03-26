.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Overview: template structure
############################

This page covers what a template looks like: what parts of a template there are, and where they go.

For the structure of a DAML file *outside* a template, see :doc:`file-structure`.

.. _daml-ref-template-structure:

Template outline structure
**************************

Here’s the structure of a DAML template:

.. literalinclude:: ../code-snippets/Structure.daml
   :start-after: -- start of template outline snippet
   :end-before: -- end of template outline snippet

:ref:`template name <daml-ref-template-name>`
    ``template`` keyword

:ref:`parameters <daml-ref-template-parameters>`
    ``with`` followed by the names of parameters and their types

template body
    ``where`` keyword

    Can include:

    :ref:`signatories <daml-ref-signatories>`
        ``signatory`` keyword

        Required. The parties (see the :ref:`Party <daml-ref-built-in-types>` type) who must consent to the creation of an instance of this contract. You won't be able to create an instance of this contract until all of these parties have authorized it.

    :ref:`observers <daml-ref-observers>`
    	``observer`` keyword

    	Optional. Parties that aren't signatories but who you still want to be able to see this contract.

    :ref:`an agreement <daml-ref-agreements>`
        ``agreement`` keyword

        Optional. Text that describes the agreement that this contract represents.

    :ref:`a precondition <daml-ref-preconditions>`
        ``ensure`` keyword

        Only create the contract if the conditions after ``ensure`` evaluate to true.

    :ref:`a contract key <daml-ref-contract-keys>`
        ``key`` keyword

        Optional. Lets you specify a combination of a party and other data that uniquely identifies an instance of this contract template. See :doc:`/daml/reference/contract-keys`.

    :ref:`maintainers <daml-ref-maintainers>`
        ``maintainer`` keyword

        Required if you have specified a ``key``. Keys are only unique to a ``maintainer``. See :doc:`/daml/reference/contract-keys`.

    :ref:`choices <daml-ref-choice-structure>`
        ``choice NameOfChoice : ReturnType controller nameOfParty do``

        or

    	``controller nameOfParty can NameOfChoice : ReturnType do``

        Defines choices that can be exercised. See `Choice structure`_ for what can go in a choice.

.. _daml-ref-choice-structure:

Choice structure
****************

Here's the structure of a choice inside a template. There are two ways of specifying a choice:

- start with the ``choice`` keyword
- start with the ``controller`` keyword

.. literalinclude:: ../code-snippets/Structure.daml
   :language: daml
   :start-after: -- start of choice snippet
   :end-before: -- end of choice snippet
   :dedent: 4

:ref:`a controller (or controllers) <daml-ref-controllers>`
    ``controller`` keyword

    Who can exercise the choice.

:ref:`consumption annotation <daml-ref-consumability>`
    Optionally one of ``preconsuming``, ``postconsuming``, ``nonconsuming``, which changes the behavior of the choice with respect to privacy and if and when the contract is archived.
    See :ref:`contract consumption in choices <daml-ref-consumability>` for more details.

:ref:`a name <daml-ref-choice-name>`
    Must begin with a capital letter. Must be unique - choices in different templates can't have the same name.

:ref:`a return type <daml-ref-return-type>`
    after a ``:``, the return type of the choice

:ref:`choice arguments <daml-ref-choice-arguments>`
    ``with`` keyword

    If you start your choice with ``choice`` and include a ``Party`` as a parameter, you can make that ``Party`` the ``controller`` of the choice. This is a feature called "flexible controllers", and it means you don't have to specify the controller when you create the contract - you can specify it when you exercise the choice. To exercise a choice, the party needs to be a signatory or an observer of the contract and must be explicitly declared as such.

:ref:`a choice body <daml-ref-choice-body>`
    After ``do`` keyword

    What happens when someone exercises the choice. A choice body can contain update statements: see `Choice body structure`_ below.

.. _daml-ref-choice-body-structure:

Choice body structure
*********************

A choice body contains ``Update`` expressions, wrapped in a :ref:`do <daml-ref-do>` block.

The update expressions are:

:ref:`create <daml-ref-create>`
    Create a new contract instance of this template.

    ``create NameOfContract with contractArgument1 = value1; contractArgument2 = value2; ...``

:ref:`exercise <daml-ref-exercise>`
    Exercise a choice on a particular contract.

    ``exercise idOfContract NameOfChoiceOnContract with choiceArgument1 = value1; choiceArgument2 = value 2; ...``

:ref:`fetch <daml-ref-fetch>`
    Fetch a contract instance using its ID. Often used with assert to check conditions on the contract’s content.

    ``fetchedContract <- fetch IdOfContract``

:ref:`fetchByKey <daml-ref-fetch-by-key>`
    Like ``fetch``, but uses a :doc:`contract key </daml/reference/contract-keys>` rather than an ID.

    ``fetchedContract <- fetchByKey @ContractType contractKey``

:ref:`lookupByKey <daml-ref-lookup-by-key>`
    Confirm that a contract with the given :doc:`contract key </daml/reference/contract-keys>` exists.

    ``fetchedContractId <- lookupByKey @ContractType contractKey``

:ref:`abort <daml-ref-abort>`
    Stop execution of the choice, fail the update.

    ``if False then abort``

:ref:`assert <daml-ref-assert>`
    Fail the update unless the condition is true. Usually used to limit the arguments that can be supplied to a contract choice.

    ``assert (amount > 0)``

:ref:`getTime <daml-ref-gettime>`
    Gets the ledger effective time. Usually used to restrict when a choice can be exercised.

    ``currentTime <- getTime``

:ref:`return <daml-ref-return>`
    Explicitly return a value. By default, a choice returns the result of its last update expression. This means you only need to use ``return`` if you want to return something else.

    ``return ContractID ExampleTemplate``

The choice body can also contain:

:ref:`let <daml-ref-let-update>` keyword
    Used to assign values or functions.

assign a value to the result of an update statement
   For example: ``contractFetched <- fetch someContractId``
