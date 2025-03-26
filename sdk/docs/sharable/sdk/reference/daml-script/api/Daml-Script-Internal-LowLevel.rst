.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-lowlevel-80672:

Daml.Script.Internal.LowLevel
=============================

Typeclasses
-----------

.. _class-daml-script-internal-lowlevel-isquestion-79227:

**class** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ req res **where**

  .. _function-daml-script-internal-lowlevel-command-29824:

  `command <function-daml-script-internal-lowlevel-command-29824_>`_
    \: `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  .. _function-daml-script-internal-lowlevel-version-95863:

  `version <function-daml-script-internal-lowlevel-version-95863_>`_
    \: `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  .. _function-daml-script-internal-lowlevel-makequestion-25300:

  `makeQuestion <function-daml-script-internal-lowlevel-makequestion-25300_>`_
    \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> req \-\> `Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res res

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`Secp256k1GenerateKeyPair <type-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-99276>` :ref:`Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395>`

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`Secp256k1Sign <type-daml-script-internal-questions-crypto-text-secp256k1sign-62642>` `BytesHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`Catch <type-daml-script-internal-questions-exceptions-catch-84605>` (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_ x)

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`FailWithStatus <type-daml-script-internal-questions-exceptions-failwithstatus-68689>` t

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`Throw <type-daml-script-internal-questions-exceptions-throw-53740>` t

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`ListAllPackages <type-daml-script-internal-questions-packages-listallpackages-28931>` \[:ref:`PackageName <type-daml-script-internal-questions-packages-packagename-68696>`\]

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`ListVettedPackages <type-daml-script-internal-questions-packages-listvettedpackages-5133>` \[:ref:`PackageName <type-daml-script-internal-questions-packages-packagename-68696>`\]

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`UnvetDar <type-daml-script-internal-questions-packages-unvetdar-94927>` ()

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`UnvetPackages <type-daml-script-internal-questions-packages-unvetpackages-98510>` ()

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`VetDar <type-daml-script-internal-questions-packages-vetdar-93380>` ()

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`VetPackages <type-daml-script-internal-questions-packages-vetpackages-30455>` ()

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`AllocateParty <type-daml-script-internal-questions-partymanagement-allocateparty-41025>` `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`ListKnownParties <type-daml-script-internal-questions-partymanagement-listknownparties-97656>` \[:ref:`PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369>`\]

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`QueryACS <type-daml-script-internal-questions-query-queryacs-99849>` \[(`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ (), `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_)\]

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`QueryContractId <type-daml-script-internal-questions-query-querycontractid-2586>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_, `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_, `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_))

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`QueryContractKey <type-daml-script-internal-questions-query-querycontractkey-66849>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ (), `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_))

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`QueryInterface <type-daml-script-internal-questions-query-queryinterface-90785>` \[`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_\]

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`QueryInterfaceContractId <type-daml-script-internal-questions-query-queryinterfacecontractid-74514>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_)

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`Submit <type-daml-script-internal-questions-submit-submit-31549>` \[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\]

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`TryCommands <type-daml-script-internal-questions-testing-trycommands-91696>` (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ (`Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_) x)

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`GetTime <type-daml-script-internal-questions-time-gettime-36498>` `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`SetTime <type-daml-script-internal-questions-time-settime-6646>` ()

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`Sleep <type-daml-script-internal-questions-time-sleep-74638>` ()

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`CreateUser <type-daml-script-internal-questions-usermanagement-createuser-632>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ ())

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`DeleteUser <type-daml-script-internal-questions-usermanagement-deleteuser-32589>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ ())

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`GetUser <type-daml-script-internal-questions-usermanagement-getuser-72497>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`User <type-daml-script-internal-questions-usermanagement-user-21930>`)

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`GrantUserRights <type-daml-script-internal-questions-usermanagement-grantuserrights-74210>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[:ref:`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475>`\])

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`ListAllUsers <type-daml-script-internal-questions-usermanagement-listallusers-79412>` \[:ref:`User <type-daml-script-internal-questions-usermanagement-user-21930>`\]

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`ListUserRights <type-daml-script-internal-questions-usermanagement-listuserrights-88601>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[:ref:`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475>`\])

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`RevokeUserRights <type-daml-script-internal-questions-usermanagement-revokeuserrights-41537>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[:ref:`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475>`\])

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`ValidateUserId <type-daml-script-internal-questions-usermanagement-validateuserid-7081>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

Data Types
----------

.. _type-daml-script-internal-lowlevel-ledgervalue-66913:

**data** `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`QueryInterface <type-daml-script-internal-questions-query-queryinterface-90785>` \[`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_\]

  **instance** `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ :ref:`QueryInterfaceContractId <type-daml-script-internal-questions-query-queryinterfacecontractid-74514>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"act\" :ref:`Catch <type-daml-script-internal-questions-exceptions-catch-84605>` (() \-\> `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"act\" :ref:`TryCommands <type-daml-script-internal-questions-testing-trycommands-91696>` `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"act\" :ref:`Catch <type-daml-script-internal-questions-exceptions-catch-84605>` (() \-\> `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"act\" :ref:`TryCommands <type-daml-script-internal-questions-testing-trycommands-91696>` `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_

.. _type-daml-script-internal-lowlevel-question-76582:

**data** `Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a

  .. _constr-daml-script-internal-lowlevel-question-60451:

  `Question <constr-daml-script-internal-lowlevel-question-60451_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - commandName
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - commandVersion
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         -
       * - payload
         - req
         -
       * - locations
         - \[(`Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `SrcLoc <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-da-stack-types-srcloc-15887>`_)\]
         -
       * - continue
         - res \-\> a
         -

  **instance** `Functor <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-base-functor-31205>`_ (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"commandName\" (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a) `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"commandVersion\" (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a) `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"continue\" (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a) (res \-\> a)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"locations\" (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a) \[(`Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `SrcLoc <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-da-stack-types-srcloc-15887>`_)\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"payload\" (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a) req

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"commandName\" (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a) `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"commandVersion\" (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a) `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"continue\" (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a) (res \-\> a)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"locations\" (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a) \[(`Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `SrcLoc <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-da-stack-types-srcloc-15887>`_)\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"payload\" (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ req res a) req

.. _type-daml-script-internal-lowlevel-script-4781:

**data** `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a

  This is the type of A Daml script\. ``Script`` is an instance of ``Action``,
  so you can use ``do`` notation\.

  .. _constr-daml-script-internal-lowlevel-script-73096:

  `Script <constr-daml-script-internal-lowlevel-script-73096_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - runScript
         - () \-\> Free `ScriptF <type-daml-script-internal-lowlevel-scriptf-37150_>`_ (a, ())
         -
       * - dummy
         - ()
         -

  **instance** :ref:`ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101>` `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `Functor <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-base-functor-31205>`_ `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `CanAssert <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-assert-canassert-67323>`_ `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `ActionCatch <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-actioncatch-69238>`_ `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `ActionThrow <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-actionthrow-37623>`_ `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `ActionFailWithStatus <https://docs.daml.com/daml/stdlib/DA-Fail.html#class-da-internal-fail-actionfailwithstatus-58664>`_ `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `CanAbort <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-lf-canabort-29060>`_ `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `HasTime <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-lf-hastime-96546>`_ `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `Action <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-action-68790>`_ `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `ActionFail <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-actionfail-34438>`_ `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `Applicative <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-applicative-9257>`_ `Script <type-daml-script-internal-lowlevel-script-4781_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"dummy\" (`Script <type-daml-script-internal-lowlevel-script-4781_>`_ a) ()

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"runScript\" (`Script <type-daml-script-internal-lowlevel-script-4781_>`_ a) (() \-\> Free `ScriptF <type-daml-script-internal-lowlevel-scriptf-37150_>`_ (a, ()))

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"dummy\" (`Script <type-daml-script-internal-lowlevel-script-4781_>`_ a) ()

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"runScript\" (`Script <type-daml-script-internal-lowlevel-script-4781_>`_ a) (() \-\> Free `ScriptF <type-daml-script-internal-lowlevel-scriptf-37150_>`_ (a, ()))

.. _type-daml-script-internal-lowlevel-scriptf-37150:

**data** `ScriptF <type-daml-script-internal-lowlevel-scriptf-37150_>`_ a

  .. _constr-daml-script-internal-lowlevel-scriptf-96157:

  `ScriptF <constr-daml-script-internal-lowlevel-scriptf-96157_>`_ (`Question <type-daml-script-internal-lowlevel-question-76582_>`_ `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_ `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_ a)


  **instance** `Functor <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-base-functor-31205>`_ `ScriptF <type-daml-script-internal-lowlevel-scriptf-37150_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"runScript\" (`Script <type-daml-script-internal-lowlevel-script-4781_>`_ a) (() \-\> Free `ScriptF <type-daml-script-internal-lowlevel-scriptf-37150_>`_ (a, ()))

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"runScript\" (`Script <type-daml-script-internal-lowlevel-script-4781_>`_ a) (() \-\> Free `ScriptF <type-daml-script-internal-lowlevel-scriptf-37150_>`_ (a, ()))

Functions
---------

.. _function-daml-script-internal-lowlevel-getexposedcallstack-93035:

`getExposedCallStack <function-daml-script-internal-lowlevel-getexposedcallstack-93035_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[(`Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `SrcLoc <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-da-stack-types-srcloc-15887>`_)\]

.. _function-daml-script-internal-lowlevel-lift-11033:

`lift <function-daml-script-internal-lowlevel-lift-11033_>`_
  \: (`HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227_>`_ req res) \=\> req \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ res

.. _function-daml-script-internal-lowlevel-script-65113:

`script <function-daml-script-internal-lowlevel-script-65113_>`_
  \: `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a

  Convenience helper to declare you are writing a Script\.

  This is only useful for readability and to improve type inference\.
  Any expression of type ``Script a`` is a valid script regardless of whether
  it is implemented using ``script`` or not\.

.. _function-daml-script-internal-lowlevel-fromledgervalue-46749:

`fromLedgerValue <function-daml-script-internal-lowlevel-fromledgervalue-46749_>`_
  \: `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_ \-\> a

.. _function-daml-script-internal-lowlevel-toledgervalue-45258:

`toLedgerValue <function-daml-script-internal-lowlevel-toledgervalue-45258_>`_
  \: a \-\> `LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913_>`_

.. _function-daml-script-internal-lowlevel-anytoanyexception-43153:

`anyToAnyException <function-daml-script-internal-lowlevel-anytoanyexception-43153_>`_
  \: Any \-\> `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_

.. _function-daml-script-internal-lowlevel-anyexceptiontoany-62585:

`anyExceptionToAny <function-daml-script-internal-lowlevel-anyexceptiontoany-62585_>`_
  \: `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_ \-\> Any

