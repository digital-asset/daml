.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-submit-61157:

Daml.Script.Internal.Questions.Submit
=====================================

Typeclasses
-----------

.. _class-daml-script-internal-questions-submit-issubmitoptions-64211:

**class** `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options **where**

  Defines a type that can be transformed into a SubmitOptions

  .. _function-daml-script-internal-questions-submit-tosubmitoptions-99319:

  `toSubmitOptions <function-daml-script-internal-questions-submit-tosubmitoptions-99319_>`_
    \: options \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  **instance** `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  **instance** `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  **instance** `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ (`NonEmpty <https://docs.daml.com/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_)

  **instance** `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ (`Set <https://docs.daml.com/daml/stdlib/DA-Set.html#type-da-set-types-set-90436>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_)

  **instance** `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

.. _class-daml-script-internal-questions-submit-scriptsubmit-55101:

**class** `Applicative <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-applicative-9257>`_ script \=\> `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script **where**

  Defines an applicative that can run transaction submissions\. Usually this is simply ``Script``\.

  .. _function-daml-script-internal-questions-submit-liftsubmission-99954:

  `liftSubmission <function-daml-script-internal-questions-submit-liftsubmission-99954_>`_
    \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a \-\> script a

  **instance** `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ :ref:`Script <type-daml-script-internal-lowlevel-script-4781>`

  **instance** `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_

Data Types
----------

.. _type-daml-script-internal-questions-submit-concurrentsubmits-82688:

**data** `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a

  Applicative that allows for multiple concurrent transaction submissions
  See ``concurrently`` for usage of this type\.

  .. _constr-daml-script-internal-questions-submit-concurrentsubmits-49827:

  `ConcurrentSubmits <constr-daml-script-internal-questions-submit-concurrentsubmits-49827_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - submits
         - \[`Submission <type-daml-script-internal-questions-submit-submission-45309_>`_\]
         -
       * - continue
         - \[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\] \-\> a
         -

  **instance** `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_

  **instance** `Functor <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-base-functor-31205>`_ `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_

  **instance** `Applicative <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-applicative-9257>`_ `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"continue\" (`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a) (\[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\] \-\> a)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"submits\" (`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a) \[`Submission <type-daml-script-internal-questions-submit-submission-45309_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"continue\" (`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a) (\[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\] \-\> a)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"submits\" (`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a) \[`Submission <type-daml-script-internal-questions-submit-submission-45309_>`_\]

.. _type-daml-script-internal-questions-submit-errorbehaviour-35454:

**data** `ErrorBehaviour <type-daml-script-internal-questions-submit-errorbehaviour-35454_>`_

  .. _constr-daml-script-internal-questions-submit-mustsucceed-57102:

  `MustSucceed <constr-daml-script-internal-questions-submit-mustsucceed-57102_>`_


  .. _constr-daml-script-internal-questions-submit-mustfail-13973:

  `MustFail <constr-daml-script-internal-questions-submit-mustfail-13973_>`_


  .. _constr-daml-script-internal-questions-submit-try-30860:

  `Try <constr-daml-script-internal-questions-submit-try-30860_>`_


  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sErrorBehaviour\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ `ErrorBehaviour <type-daml-script-internal-questions-submit-errorbehaviour-35454_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sErrorBehaviour\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ `ErrorBehaviour <type-daml-script-internal-questions-submit-errorbehaviour-35454_>`_

.. _type-daml-script-internal-questions-submit-submission-45309:

**data** `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_

  .. _constr-daml-script-internal-questions-submit-submission-76992:

  `Submission <constr-daml-script-internal-questions-submit-submission-76992_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - sActAs
         - `NonEmpty <https://docs.daml.com/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
         -
       * - sReadAs
         - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - sDisclosures
         - \[:ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>`\]
         -
       * - sPackagePreference
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[PackageId\]
         -
       * - sPrefetchKeys
         - \[`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]
         -
       * - sErrorBehaviour
         - `ErrorBehaviour <type-daml-script-internal-questions-submit-errorbehaviour-35454_>`_
         -
       * - sCommands
         - \[:ref:`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560>`\]
         -
       * - sLocation
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `SrcLoc <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-da-stack-types-srcloc-15887>`_)
         -

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sActAs\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ (`NonEmpty <https://docs.daml.com/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sCommands\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ \[:ref:`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560>`\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sDisclosures\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ \[:ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>`\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sErrorBehaviour\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ `ErrorBehaviour <type-daml-script-internal-questions-submit-errorbehaviour-35454_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sLocation\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `SrcLoc <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-da-stack-types-srcloc-15887>`_))

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sPackagePreference\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[PackageId\])

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sPrefetchKeys\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ \[`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sReadAs\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"submissions\" `Submit <type-daml-script-internal-questions-submit-submit-31549_>`_ \[`Submission <type-daml-script-internal-questions-submit-submission-45309_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"submits\" (`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a) \[`Submission <type-daml-script-internal-questions-submit-submission-45309_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sActAs\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ (`NonEmpty <https://docs.daml.com/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sCommands\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ \[:ref:`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560>`\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sDisclosures\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ \[:ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>`\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sErrorBehaviour\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ `ErrorBehaviour <type-daml-script-internal-questions-submit-errorbehaviour-35454_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sLocation\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `SrcLoc <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-da-stack-types-srcloc-15887>`_))

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sPackagePreference\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[PackageId\])

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sPrefetchKeys\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ \[`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sReadAs\" `Submission <type-daml-script-internal-questions-submit-submission-45309_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"submissions\" `Submit <type-daml-script-internal-questions-submit-submit-31549_>`_ \[`Submission <type-daml-script-internal-questions-submit-submission-45309_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"submits\" (`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a) \[`Submission <type-daml-script-internal-questions-submit-submission-45309_>`_\]

.. _type-daml-script-internal-questions-submit-submit-31549:

**data** `Submit <type-daml-script-internal-questions-submit-submit-31549_>`_

  .. _constr-daml-script-internal-questions-submit-submit-5176:

  `Submit <constr-daml-script-internal-questions-submit-submit-5176_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - submissions
         - \[`Submission <type-daml-script-internal-questions-submit-submission-45309_>`_\]
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `Submit <type-daml-script-internal-questions-submit-submit-31549_>`_ \[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"submissions\" `Submit <type-daml-script-internal-questions-submit-submit-31549_>`_ \[`Submission <type-daml-script-internal-questions-submit-submission-45309_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"submissions\" `Submit <type-daml-script-internal-questions-submit-submit-31549_>`_ \[`Submission <type-daml-script-internal-questions-submit-submission-45309_>`_\]

.. _type-daml-script-internal-questions-submit-submitoptions-56692:

**data** `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Options to detemine the stakeholders of a transaction, as well as disclosures\.
  Intended to be specified using the ``actAs``, ``readAs`` and ``disclose`` builders, combined using the Semigroup concat ``(<>)`` operator\.

  .. code-block:: daml

    actAs alice <> readAs [alice, bob] <> disclose myContract


  Note that actAs and readAs follows the same party derivation rules as ``signatory``, see their docs for examples\.
  All submissions must specify at least one ``actAs`` party, else a runtime error will be thrown\.
  A minimum submission may look like

  .. code-block:: daml

    submit (actAs alice) $ createCmd MyContract with party = alice

  .. _constr-daml-script-internal-questions-submit-submitoptions-37975:

  `SubmitOptions <constr-daml-script-internal-questions-submit-submitoptions-37975_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - soActAs
         - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - soReadAs
         - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - soDisclosures
         - \[:ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>`\]
         -
       * - soPackagePreference
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[PackageId\]
         -
       * - soPrefetchKeys
         - \[`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]
         -

  **instance** `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  **instance** `Monoid <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-monoid-6742>`_ `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  **instance** `Semigroup <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-semigroup-78998>`_ `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"soActAs\" `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"soDisclosures\" `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ \[:ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>`\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"soPackagePreference\" `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[PackageId\])

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"soPrefetchKeys\" `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ \[`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"soReadAs\" `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"soActAs\" `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"soDisclosures\" `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ \[:ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>`\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"soPackagePreference\" `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[PackageId\])

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"soPrefetchKeys\" `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ \[`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"soReadAs\" `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

.. _type-daml-script-internal-questions-submit-timeboundedresult-96051:

**data** `TimeBoundedResult <type-daml-script-internal-questions-submit-timeboundedresult-96051_>`_ a

  .. _constr-daml-script-internal-questions-submit-timeboundedresult-3036:

  `TimeBoundedResult <constr-daml-script-internal-questions-submit-timeboundedresult-3036_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - value
         - a
         -
       * - minLedgerTime
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_
         -
       * - maxLedgerTime
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_
         -

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ a \=\> `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ (`TimeBoundedResult <type-daml-script-internal-questions-submit-timeboundedresult-96051_>`_ a)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"maxLedgerTime\" (`TimeBoundedResult <type-daml-script-internal-questions-submit-timeboundedresult-96051_>`_ a) (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"minLedgerTime\" (`TimeBoundedResult <type-daml-script-internal-questions-submit-timeboundedresult-96051_>`_ a) (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"value\" (`TimeBoundedResult <type-daml-script-internal-questions-submit-timeboundedresult-96051_>`_ a) a

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"maxLedgerTime\" (`TimeBoundedResult <type-daml-script-internal-questions-submit-timeboundedresult-96051_>`_ a) (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"minLedgerTime\" (`TimeBoundedResult <type-daml-script-internal-questions-submit-timeboundedresult-96051_>`_ a) (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"value\" (`TimeBoundedResult <type-daml-script-internal-questions-submit-timeboundedresult-96051_>`_ a) a

Functions
---------

.. _function-daml-script-internal-questions-submit-combineoptional-54959:

`combineOptional <function-daml-script-internal-questions-submit-combineoptional-54959_>`_
  \: `Semigroup <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-semigroup-78998>`_ a \=\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ a \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ a \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ a

.. _function-daml-script-internal-questions-submit-actas-76494:

`actAs <function-daml-script-internal-questions-submit-actas-76494_>`_
  \: `IsParties <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ parties \=\> parties \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Builds a SubmitOptions with given actAs parties\.
  Any given submission must include at least one actAs party\.
  Note that the parties type is constrainted by ``IsParties``, allowing for specifying parties as any of the following\:

  .. code-block:: daml

    Party
    [Party]
    NonEmpty Party
    Set Party
    Optional Party

.. _function-daml-script-internal-questions-submit-readas-67481:

`readAs <function-daml-script-internal-questions-submit-readas-67481_>`_
  \: `IsParties <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ parties \=\> parties \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Builds a SubmitOptions with given readAs parties\.
  A given submission may omit any readAs parties and still be valid\.
  Note that the parties type is constrainted by ``IsParties``, allowing for specifying parties as any of the following\:

  .. code-block:: daml

    Party
    [Party]
    NonEmpty Party
    Set Party
    Optional Party

.. _function-daml-script-internal-questions-submit-disclosemany-53386:

`discloseMany <function-daml-script-internal-questions-submit-disclosemany-53386_>`_
  \: \[:ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>`\] \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Provides many Explicit Disclosures to the transaction\.

.. _function-daml-script-internal-questions-submit-disclose-59895:

`disclose <function-daml-script-internal-questions-submit-disclose-59895_>`_
  \: :ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>` \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Provides an Explicit Disclosure to the transaction\.

.. _function-daml-script-internal-questions-submit-packagepreference-25445:

`packagePreference <function-daml-script-internal-questions-submit-packagepreference-25445_>`_
  \: \[PackageId\] \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Provide a package id selection preference for upgrades for a submission

.. _function-daml-script-internal-questions-submit-prefetchkeys-84998:

`prefetchKeys <function-daml-script-internal-questions-submit-prefetchkeys-84998_>`_
  \: \[`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\] \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Provide a list of contract keys to prefetch for a submission

.. _function-daml-script-internal-questions-submit-actasnonempty-25978:

`actAsNonEmpty <function-daml-script-internal-questions-submit-actasnonempty-25978_>`_
  \: \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> `NonEmpty <https://docs.daml.com/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

.. _function-daml-script-internal-questions-submit-concurrently-75077:

`concurrently <function-daml-script-internal-questions-submit-concurrently-75077_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a

  Allows for concurrent submission of transactions, using an applicative, similar to Commands\.
  Concurrently takes a computation in ``ConcurrentSubmits``, which supports all the existing ``submit`` functions
  that ``Script`` supports\. It however does not implement ``Action``, and thus does not support true binding and computation interdependence
  NOTE\: The submission order of transactions within ``concurrently`` is deterministic, this function is not intended to test contention\.
  It is only intended to allow faster submission of many unrelated transactions, by not waiting for completion for each transaction before
  sending the next\.
  Example\:

  .. code-block:: daml

    exerciseResult <- concurrently $ do
      alice `submit` createCmd ...
      res <- alice `submit` exerciseCmd ...
      bob `submit` createCmd ...
      pure res

.. _function-daml-script-internal-questions-submit-submitinternal-46082:

`submitInternal <function-daml-script-internal-questions-submit-submitinternal-46082_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_ \-\> `ErrorBehaviour <type-daml-script-internal-questions-submit-errorbehaviour-35454_>`_ \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (a, :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`))

.. _function-daml-script-internal-questions-submit-mustsucceed-76821:

`mustSucceed <function-daml-script-internal-questions-submit-mustsucceed-76821_>`_
  \: `Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ a b \-\> b

.. _function-daml-script-internal-questions-submit-mustfail-99636:

`mustFail <function-daml-script-internal-questions-submit-mustfail-99636_>`_
  \: `Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ a b \-\> a

.. _function-daml-script-internal-questions-submit-submitresultandtree-13546:

`submitResultAndTree <function-daml-script-internal-questions-submit-submitresultandtree-13546_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script (a, :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)

  Equivalent to ``submit`` but returns the result and the full transaction tree\.

.. _function-daml-script-internal-questions-submit-trysubmitresultandtree-33682:

`trySubmitResultAndTree <function-daml-script-internal-questions-submit-trysubmitresultandtree-33682_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (a, :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`))

  Equivalent to ``trySubmit`` but returns the result and the full transaction tree\.

.. _function-daml-script-internal-questions-submit-submitwitherror-52958:

`submitWithError <function-daml-script-internal-questions-submit-submitwitherror-52958_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>`

  Equivalent to ``submitMustFail`` but returns the error thrown\.

.. _function-daml-script-internal-questions-submit-submit-5889:

`submit <function-daml-script-internal-questions-submit-submit-5889_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script a

  ``submit p cmds`` submits the commands ``cmds`` as a single transaction
  from party ``p`` and returns the value returned by ``cmds``\.
  The ``options`` field can either be any \"Parties\" like type (See ``IsParties``) or ``SubmitOptions``
  which allows for finer control over parameters of the submission\.

  If the transaction fails, ``submit`` also fails\.

.. _function-daml-script-internal-questions-submit-submitwithoptions-56152:

`submitWithOptions <function-daml-script-internal-questions-submit-submitwithoptions-56152_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script a

.. _function-daml-script-internal-questions-submit-submittree-5925:

`submitTree <function-daml-script-internal-questions-submit-submittree-5925_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`

  Equivalent to ``submit`` but returns the full transaction tree\.

.. _function-daml-script-internal-questions-submit-trysubmit-23693:

`trySubmit <function-daml-script-internal-questions-submit-trysubmit-23693_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` a)

  Submit a transaction and receive back either the result, or a ``SubmitError``\.
  In the majority of failures, this will not crash at runtime\.

.. _function-daml-script-internal-questions-submit-trysubmittree-68085:

`trySubmitTree <function-daml-script-internal-questions-submit-trysubmittree-68085_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)

  Equivalent to ``trySubmit`` but returns the full transaction tree\.

.. _function-daml-script-internal-questions-submit-submitmustfail-63662:

`submitMustFail <function-daml-script-internal-questions-submit-submitmustfail-63662_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script ()

  ``submitMustFail p cmds`` submits the commands ``cmds`` as a single transaction
  from party ``p``\.
  See submitWithOptions for details on the ``options`` field

  It only succeeds if the submitting the transaction fails\.

.. _function-daml-script-internal-questions-submit-submitmustfailwithoptions-20017:

`submitMustFailWithOptions <function-daml-script-internal-questions-submit-submitmustfailwithoptions-20017_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script ()

.. _function-daml-script-internal-questions-submit-extsubmit-94127:

`extSubmit <function-daml-script-internal-questions-submit-extsubmit-94127_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options, `Action <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-action-68790>`_ script) \=\> options \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script (`TimeBoundedResult <type-daml-script-internal-questions-submit-timeboundedresult-96051_>`_ a)

  An extended version of ``submit`` that also returns additional information about the result of the
  submission, which mirrors the information that would be available on the prepared transaction\.

.. _function-daml-script-internal-questions-submit-submitmulti-45107:

`submitMulti <function-daml-script-internal-questions-submit-submitmulti-45107_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script a

  ``submitMulti actAs readAs cmds`` submits ``cmds`` as a single transaction
  authorized by ``actAs``\. Fetched contracts must be visible to at least
  one party in the union of actAs and readAs\.

  Note\: This behaviour can be achieved using ``submit (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-submitmultimustfail-77808:

`submitMultiMustFail <function-daml-script-internal-questions-submit-submitmultimustfail-77808_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script ()

  ``submitMultiMustFail actAs readAs cmds`` behaves like ``submitMulti actAs readAs cmds``
  but fails when ``submitMulti`` succeeds and the other way around\.

  Note\: This behaviour can be achieved using ``submitMustFail (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-submittreemulti-4879:

`submitTreeMulti <function-daml-script-internal-questions-submit-submittreemulti-4879_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`

  Equivalent to ``submitMulti`` but returns the full transaction tree\.

  Note\: This behaviour can be achieved using ``submitTree (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-trysubmitmulti-31939:

`trySubmitMulti <function-daml-script-internal-questions-submit-trysubmitmulti-31939_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> script (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` a)

  Alternate version of ``trySubmit`` that allows specifying the actAs and readAs parties\.

  Note\: This behaviour can be achieved using ``trySubmit (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-trysubmitconcurrently-11443:

`trySubmitConcurrently <function-daml-script-internal-questions-submit-trysubmitconcurrently-11443_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> \[:ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` a\]

.. _function-daml-script-internal-questions-submit-submitwithdisclosures-50120:

`submitWithDisclosures <function-daml-script-internal-questions-submit-submitwithdisclosures-50120_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> \[:ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>`\] \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a

.. _function-daml-script-internal-questions-submit-submitwithdisclosuresmustfail-28475:

`submitWithDisclosuresMustFail <function-daml-script-internal-questions-submit-submitwithdisclosuresmustfail-28475_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> \[:ref:`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298>`\] \-\> :ref:`Commands <type-daml-script-internal-questions-commands-commands-79301>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

