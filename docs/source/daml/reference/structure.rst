.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Overview: template structure
############################

This page covers what a template looks like: what parts of a template there are, and where they go.

For the structure of a DAML file *outside* a template, see :doc:`file-structure`.

.. contents:: :local:

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

        The parties (see the :ref:`Party <daml-ref-built-in-types>` type) who must consent to the creation of an instance of this contract.

    :ref:`observers <daml-ref-observers>`
    	``observer`` keyword

    	Parties that aren't signatories but can still see this contract.

    :ref:`an agreement <daml-ref-agreements>`
        ``agreement`` keyword

        Text that describes the agreement that this contract represents.

    :ref:`a precondition <daml-ref-preconditions>`
        ``ensure`` keyword

        Only create the contract if the conditions after ``ensure`` evaluate to true.

    :ref:`choices <daml-ref-choice-structure>`
    	``controller nameOfParty can nameOfChoice do``

        or

        ``choice : () controller do``

        Defines choices that can be exercised. See `Choice structure`_ for what can go in a choice.

.. _daml-ref-choice-structure:

Choice structure
****************

Here's the structure of a choice inside a template. There are two ways of specifying a choice:

- start with the ``choice`` keyword
- start with the ``controller`` keyword

TODO compare and contrast

.. literalinclude:: ../code-snippets/Structure.daml
   :language: daml
   :start-after: -- start of choice snippet
   :end-before: -- end of choice snippet
   :dedent: 4

:ref:`a controller (or controllers) <daml-ref-controllers>`
    ``controller`` keyword

    Who can exercise the choice. TODO explain flexible controllers

:ref:`consumability <daml-ref-anytime>`
    ``nonconsuming`` keyword

    By default, contracts are archived when a choice on them is exercised. If you include ``nonconsuming``, this choice can be exercised over and over.

:ref:`a name <daml-ref-choice-name>`
    Must begin with a capital letter. Must be unique - choices in different templates can't have the same name.

:ref:`a return type <daml-ref-return-type>`
    after a ``:``, the return type of the choice

:ref:`choice arguments <daml-ref-choice-arguments>`
    ``with`` keyword

    TODO: explain about passing party in

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

    ``return amount``

The choice body can also contain:

:ref:`let <daml-ref-let-update>` keyword
    Used to assign values or functions.

    .. Changes in DAML 1.2.

assign a value to the result of an update statement
   For example: ``contractFetched <- fetch someContractId``
