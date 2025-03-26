.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-query-57736:

Daml.Script.Internal.Questions.Query
====================================

Data Types
----------

.. _type-daml-script-internal-questions-query-queryacs-99849:

**data** `QueryACS <type-daml-script-internal-questions-query-queryacs-99849_>`_

  .. _constr-daml-script-internal-questions-query-queryacs-69610:

  `QueryACS <constr-daml-script-internal-questions-query-queryacs-69610_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - parties
         - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - tplId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `QueryACS <type-daml-script-internal-questions-query-queryacs-99849_>`_ \[(`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ (), `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_)\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"parties\" `QueryACS <type-daml-script-internal-questions-query-queryacs-99849_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"tplId\" `QueryACS <type-daml-script-internal-questions-query-queryacs-99849_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"parties\" `QueryACS <type-daml-script-internal-questions-query-queryacs-99849_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"tplId\" `QueryACS <type-daml-script-internal-questions-query-queryacs-99849_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-query-querycontractid-2586:

**data** `QueryContractId <type-daml-script-internal-questions-query-querycontractid-2586_>`_

  .. _constr-daml-script-internal-questions-query-querycontractid-39119:

  `QueryContractId <constr-daml-script-internal-questions-query-querycontractid-39119_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - parties
         - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - tplId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - cid
         - `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ()
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `QueryContractId <type-daml-script-internal-questions-query-querycontractid-2586_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_, `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_, `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_))

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"cid\" `QueryContractId <type-daml-script-internal-questions-query-querycontractid-2586_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"parties\" `QueryContractId <type-daml-script-internal-questions-query-querycontractid-2586_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"tplId\" `QueryContractId <type-daml-script-internal-questions-query-querycontractid-2586_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"cid\" `QueryContractId <type-daml-script-internal-questions-query-querycontractid-2586_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"parties\" `QueryContractId <type-daml-script-internal-questions-query-querycontractid-2586_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"tplId\" `QueryContractId <type-daml-script-internal-questions-query-querycontractid-2586_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-query-querycontractkey-66849:

**data** `QueryContractKey <type-daml-script-internal-questions-query-querycontractkey-66849_>`_

  .. _constr-daml-script-internal-questions-query-querycontractkey-47898:

  `QueryContractKey <constr-daml-script-internal-questions-query-querycontractkey-47898_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - parties
         - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - tplId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - key
         - `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `QueryContractKey <type-daml-script-internal-questions-query-querycontractkey-66849_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ (), `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_))

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"key\" `QueryContractKey <type-daml-script-internal-questions-query-querycontractkey-66849_>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"parties\" `QueryContractKey <type-daml-script-internal-questions-query-querycontractkey-66849_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"tplId\" `QueryContractKey <type-daml-script-internal-questions-query-querycontractkey-66849_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"key\" `QueryContractKey <type-daml-script-internal-questions-query-querycontractkey-66849_>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"parties\" `QueryContractKey <type-daml-script-internal-questions-query-querycontractkey-66849_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"tplId\" `QueryContractKey <type-daml-script-internal-questions-query-querycontractkey-66849_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-query-queryinterface-90785:

**data** `QueryInterface <type-daml-script-internal-questions-query-queryinterface-90785_>`_

  .. _constr-daml-script-internal-questions-query-queryinterface-39438:

  `QueryInterface <constr-daml-script-internal-questions-query-queryinterface-39438_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - parties
         - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - interfaceId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `QueryInterface <type-daml-script-internal-questions-query-queryinterface-90785_>`_ \[:ref:`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913>`\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"interfaceId\" `QueryInterface <type-daml-script-internal-questions-query-queryinterface-90785_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"parties\" `QueryInterface <type-daml-script-internal-questions-query-queryinterface-90785_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"interfaceId\" `QueryInterface <type-daml-script-internal-questions-query-queryinterface-90785_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"parties\" `QueryInterface <type-daml-script-internal-questions-query-queryinterface-90785_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

.. _type-daml-script-internal-questions-query-queryinterfacecontractid-74514:

**data** `QueryInterfaceContractId <type-daml-script-internal-questions-query-queryinterfacecontractid-74514_>`_

  .. _constr-daml-script-internal-questions-query-queryinterfacecontractid-10585:

  `QueryInterfaceContractId <constr-daml-script-internal-questions-query-queryinterfacecontractid-10585_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - parties
         - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - interfaceId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - cid
         - `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ()
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `QueryInterfaceContractId <type-daml-script-internal-questions-query-queryinterfacecontractid-74514_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913>`)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"cid\" `QueryInterfaceContractId <type-daml-script-internal-questions-query-queryinterfacecontractid-74514_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"interfaceId\" `QueryInterfaceContractId <type-daml-script-internal-questions-query-queryinterfacecontractid-74514_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"parties\" `QueryInterfaceContractId <type-daml-script-internal-questions-query-queryinterfacecontractid-74514_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"cid\" `QueryInterfaceContractId <type-daml-script-internal-questions-query-queryinterfacecontractid-74514_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"interfaceId\" `QueryInterfaceContractId <type-daml-script-internal-questions-query-queryinterfacecontractid-74514_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"parties\" `QueryInterfaceContractId <type-daml-script-internal-questions-query-queryinterfacecontractid-74514_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

Functions
---------

.. _function-daml-script-internal-questions-query-query-55941:

`query <function-daml-script-internal-questions-query-query-55941_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t, `IsParties <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[(`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, t)\]

  Query the set of active contracts of the template
  that are visible to the given party\.

.. _function-daml-script-internal-questions-query-queryfilter-99157:

`queryFilter <function-daml-script-internal-questions-query-queryfilter-99157_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ c, `HasEnsure <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ c, `IsParties <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> (c \-\> `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_) \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[(`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ c, c)\]

  Query the set of active contracts of the template
  that are visible to the given party and match the given predicate\.

.. _function-daml-script-internal-questions-query-querycontractid-7634:

`queryContractId_ <function-daml-script-internal-questions-query-querycontractid-7634_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `IsParties <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p, `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_) \=\> p \-\> `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_, `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_, `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_))

  Query for the contract with the given contract id\.

  Returns ``None`` if there is no active contract the party is a stakeholder on\.
  Otherwise returns a triplet (anyTemplate, templateId, blob) where anyTemplate
  is the contract upgraded or downgraded to ``t``, templateId is the ID of the
  template as stored in the ledger (may be different from ``t``), and blob is the
  disclosure of the template as stored in the ledger (of type templateId)\.

  WARNING\: Over the gRPC and with the JSON API
  in\-memory backend this performs a linear search so only use this if the number of
  active contracts is small\.

  This is semantically equivalent to calling ``query``
  and filtering on the client side\.

.. _function-daml-script-internal-questions-query-querycontractid-24166:

`queryContractId <function-daml-script-internal-questions-query-querycontractid-24166_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t, `IsParties <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p, `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_) \=\> p \-\> `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ t)

.. _function-daml-script-internal-questions-query-querydisclosure-12000:

`queryDisclosure <function-daml-script-internal-questions-query-querydisclosure-12000_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `IsParties <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p, `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_) \=\> p \-\> `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>`)

.. _function-daml-script-internal-questions-query-queryinterface-52085:

`queryInterface <function-daml-script-internal-questions-query-queryinterface-52085_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ i, `HasInterfaceView <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492>`_ i v, `IsParties <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[(`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ i, `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ v)\]

  Query the set of active contract views for an interface
  that are visible to the given party\.
  If the view function fails for a given contract id, The ``Optional v`` will be ``None``\.

  WARNING\: Information about instances with failed\-views is not currently returned over the JSON API\: the ``Optional v`` will be ``Some _`` for every element in the returned list\.

.. _function-daml-script-internal-questions-query-queryinterfacecontractid-18438:

`queryInterfaceContractId <function-daml-script-internal-questions-query-queryinterfacecontractid-18438_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ i, `HasInterfaceView <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492>`_ i v, `IsParties <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p, `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_) \=\> p \-\> `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ i \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ v)

  Query for the contract view with the given contract id\.

  Returns ``None`` if there is no active contract the party is a stakeholder on\.

  Returns ``None`` if the view function fails for the given contract id\.

  WARNING\: Over the gRPC and with the JSON API
  in\-memory backend this performs a linear search so only use this if the number of
  active contracts is small\.

  This is semantically equivalent to calling ``queryInterface``
  and filtering on the client side\.

.. _function-daml-script-internal-questions-query-querycontractkey-51277:

`queryContractKey <function-daml-script-internal-questions-query-querycontractkey-51277_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `TemplateKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `IsParties <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> k \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, t))

  Returns ``None`` if there is no active contract with the given key that
  the party is a stakeholder on\.

  WARNING\: Over the gRPC and with the JSON API
  in\-memory backend this performs a linear search so only use this if the number of
  active contracts is small\.

  This is semantically equivalent to calling ``query``
  and filtering on the client side\.

