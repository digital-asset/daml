.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-prelude-72703:

Prelude
=======

The pieces that make up the Daml language\.

Typeclasses
-----------

.. _class-da-internal-assert-canassert-67323:

**class** `Action <class-da-internal-prelude-action-68790_>`_ m \=\> `CanAssert <class-da-internal-assert-canassert-67323_>`_ m **where**

  Constraint that determines whether an assertion can be made
  in this context\.

  .. _function-da-internal-assert-assertfail-25389:

  `assertFail <function-da-internal-assert-assertfail-25389_>`_
    \: `Text <type-ghc-types-text-51952_>`_ \-\> m t

    Abort since an assertion has failed\. In an Update, Scenario,
    or Script context this will throw an AssertionFailed
    exception\. In an ``Either Text`` context, this will return the
    message as an error\.

  **instance** `CanAssert <class-da-internal-assert-canassert-67323_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** `CanAssert <class-da-internal-assert-canassert-67323_>`_ (`Either <type-da-types-either-56020_>`_ `Text <type-ghc-types-text-51952_>`_)

.. _class-da-internal-interface-hasinterfacetyperep-84221:

**class** `HasInterfaceTypeRep <class-da-internal-interface-hasinterfacetyperep-84221_>`_ i **where**

  (Daml\-LF \>\= 1\.15) Exposes the ``interfaceTypeRep`` function\. Available only for interfaces\.

.. _class-da-internal-interface-hastointerface-68104:

**class** `HasToInterface <class-da-internal-interface-hastointerface-68104_>`_ t i **where**

  (Daml\-LF \>\= 1\.15) Exposes the ``toInterface`` and ``toInterfaceContractId`` functions\.

.. _class-da-internal-interface-hasfrominterface-43863:

**class** `HasFromInterface <class-da-internal-interface-hasfrominterface-43863_>`_ t i **where**

  (Daml\-LF \>\= 1\.15) Exposes ``fromInterface`` and ``fromInterfaceContractId``
  functions\.

  .. _function-da-internal-interface-frominterface-94157:

  `fromInterface <function-da-internal-interface-frominterface-94157_>`_
    \: i \-\> `Optional <type-da-internal-prelude-optional-37153_>`_ t

    (Daml\-LF \>\= 1\.15) Attempt to convert an interface value back into a
    template value\. A ``None`` indicates that the expected template
    type doesn't match the underyling template type for the
    interface value\.

    For example, ``fromInterface @MyTemplate value`` will try to convert
    the interface value ``value`` into the template type ``MyTemplate``\.

.. _class-da-internal-interface-hasinterfaceview-4492:

**class** `HasInterfaceView <class-da-internal-interface-hasinterfaceview-4492_>`_ i v **where**

  .. _function-da-internal-interface-view-89932:

  `_view <function-da-internal-interface-view-89932_>`_
    \: i \-\> v

.. _class-da-internal-lf-hastime-96546:

**class** `HasTime <class-da-internal-lf-hastime-96546_>`_ m **where**

  The ``HasTime`` class is for where the time is available\: ``Update``

  .. _function-da-internal-lf-gettime-65753:

  `getTime <function-da-internal-lf-gettime-65753_>`_
    \: :ref:`HasCallStack <type-ghc-stack-types-hascallstack-63713>` \=\> m `Time <type-da-internal-lf-time-63886_>`_

    Get the current time\.

  **instance** `HasTime <class-da-internal-lf-hastime-96546_>`_ `Update <type-da-internal-lf-update-68072_>`_

.. _class-da-internal-lf-canabort-29060:

**class** `Action <class-da-internal-prelude-action-68790_>`_ m \=\> `CanAbort <class-da-internal-lf-canabort-29060_>`_ m **where**

  The ``CanAbort`` class is for ``Action`` s that can be aborted\.

  .. _function-da-internal-lf-abort-93286:

  `abort <function-da-internal-lf-abort-93286_>`_
    \: `Text <type-ghc-types-text-51952_>`_ \-\> m a

    Abort the current action with a message\.

  **instance** `CanAbort <class-da-internal-lf-canabort-29060_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** `CanAbort <class-da-internal-lf-canabort-29060_>`_ (`Either <type-da-types-either-56020_>`_ `Text <type-ghc-types-text-51952_>`_)

.. _class-da-internal-prelude-applicative-9257:

**class** `Functor <class-ghc-base-functor-31205_>`_ f \=\> `Applicative <class-da-internal-prelude-applicative-9257_>`_ f **where**

  .. _function-da-internal-prelude-pure-90071:

  `pure <function-da-internal-prelude-pure-90071_>`_
    \: a \-\> f a

    Lift a value\.

  .. _function-da-internal-prelude-ltstargt-38714:

  `(<*>) <function-da-internal-prelude-ltstargt-38714_>`_
    \: f (a \-\> b) \-\> f a \-\> f b

    Sequentially apply the function\.

    A few functors support an implementation of ``<*>`` that is more
    efficient than the default one\.

  .. _function-da-internal-prelude-lifta2-20157:

  `liftA2 <function-da-internal-prelude-lifta2-20157_>`_
    \: (a \-\> b \-\> c) \-\> f a \-\> f b \-\> f c

    Lift a binary function to actions\.

    Some functors support an implementation of ``liftA2`` that is more
    efficient than the default one\. In particular, if ``fmap`` is an
    expensive operation, it is likely better to use ``liftA2`` than to
    ``fmap`` over the structure and then use ``<*>``\.

  .. _function-da-internal-prelude-stargt-24971:

  `(*>) <function-da-internal-prelude-stargt-24971_>`_
    \: f a \-\> f b \-\> f b

    Sequence actions, discarding the value of the first argument\.

  .. _function-da-internal-prelude-ltstar-15465:

  `(<*) <function-da-internal-prelude-ltstar-15465_>`_
    \: f a \-\> f b \-\> f a

    Sequence actions, discarding the value of the second argument\.

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ ((\-\>) r)

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ (:ref:`State <type-da-action-state-type-state-76783>` s)

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ `Down <type-da-internal-down-down-61433_>`_

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ `Optional <type-da-internal-prelude-optional-37153_>`_

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ :ref:`Formula <type-da-logic-types-formula-34794>`

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>`

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ (:ref:`Validation <type-da-validation-types-validation-39644>` err)

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ (`Either <type-da-types-either-56020_>`_ e)

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ `([]) <type-ghc-types-x-2599_>`_

.. _class-da-internal-prelude-action-68790:

**class** `Applicative <class-da-internal-prelude-applicative-9257_>`_ m \=\> `Action <class-da-internal-prelude-action-68790_>`_ m **where**

  .. _function-da-internal-prelude-gtgteq-32767:

  `(>>=) <function-da-internal-prelude-gtgteq-32767_>`_
    \: m a \-\> (a \-\> m b) \-\> m b

    Sequentially compose two actions, passing any value produced
    by the first as an argument to the second\.

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ ((\-\>) r)

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ (:ref:`State <type-da-action-state-type-state-76783>` s)

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ `Down <type-da-internal-down-down-61433_>`_

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ `Optional <type-da-internal-prelude-optional-37153_>`_

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ :ref:`Formula <type-da-logic-types-formula-34794>`

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>`

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ (`Either <type-da-types-either-56020_>`_ e)

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ `([]) <type-ghc-types-x-2599_>`_

.. _class-da-internal-prelude-actionfail-34438:

**class** `Action <class-da-internal-prelude-action-68790_>`_ m \=\> `ActionFail <class-da-internal-prelude-actionfail-34438_>`_ m **where**

  This class exists to desugar pattern matches in do\-notation\.
  Polymorphic usage, or calling ``fail`` directly, is not recommended\.
  Instead consider using ``CanAbort``\.

  .. _function-da-internal-prelude-fail-40565:

  `fail <function-da-internal-prelude-fail-40565_>`_
    \: `Text <type-ghc-types-text-51952_>`_ \-\> m a

    Fail with an error message\.

  **instance** `ActionFail <class-da-internal-prelude-actionfail-34438_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** `ActionFail <class-da-internal-prelude-actionfail-34438_>`_ `Optional <type-da-internal-prelude-optional-37153_>`_

  **instance** `ActionFail <class-da-internal-prelude-actionfail-34438_>`_ (`Either <type-da-types-either-56020_>`_ `Text <type-ghc-types-text-51952_>`_)

  **instance** `ActionFail <class-da-internal-prelude-actionfail-34438_>`_ `([]) <type-ghc-types-x-2599_>`_

.. _class-da-internal-prelude-semigroup-78998:

**class** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ a **where**

  The class of semigroups (types with an associative binary operation)\.

  .. _function-da-internal-prelude-ltgt-2365:

  `(<>) <function-da-internal-prelude-ltgt-2365_>`_
    \: a \-\> a \-\> a

    An associative operation\.

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ k \=\> `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (`Map <type-da-internal-lf-map-90052_>`_ k v)

  **instance** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (`TextMap <type-da-internal-lf-textmap-11691_>`_ b)

  **instance** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ :ref:`All <type-da-monoid-types-all-38142>`

  **instance** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ :ref:`Any <type-da-monoid-types-any-3989>`

  **instance** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (:ref:`Endo <type-da-monoid-types-endo-95420>` a)

  **instance** `Multiplicative <class-ghc-num-multiplicative-10593_>`_ a \=\> `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (:ref:`Product <type-da-monoid-types-product-66754>` a)

  **instance** `Additive <class-ghc-num-additive-25881_>`_ a \=\> `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (:ref:`Sum <type-da-monoid-types-sum-76394>` a)

  **instance** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (:ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ a \=\> `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (:ref:`Max <type-da-semigroup-types-max-52699>` a)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ a \=\> `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (:ref:`Min <type-da-semigroup-types-min-78217>` a)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ k \=\> `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (:ref:`Set <type-da-set-types-set-90436>` k)

  **instance** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (:ref:`Validation <type-da-validation-types-validation-39644>` err a)

  **instance** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ `Ordering <type-ghc-types-ordering-35353_>`_

  **instance** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ `Text <type-ghc-types-text-51952_>`_

  **instance** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ \[a\]

.. _class-da-internal-prelude-monoid-6742:

**class** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ a \=\> `Monoid <class-da-internal-prelude-monoid-6742_>`_ a **where**

  The class of monoids (types with an associative binary operation that has an identity)\.

  .. _function-da-internal-prelude-mempty-31919:

  `mempty <function-da-internal-prelude-mempty-31919_>`_
    \: a

    Identity of ``(<>)``

  .. _function-da-internal-prelude-mconcat-59411:

  `mconcat <function-da-internal-prelude-mconcat-59411_>`_
    \: \[a\] \-\> a

    Fold a list using the monoid\.
    For example using ``mconcat`` on a list of strings would concatenate all strings to one lone string\.

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ k \=\> `Monoid <class-da-internal-prelude-monoid-6742_>`_ (`Map <type-da-internal-lf-map-90052_>`_ k v)

  **instance** `Monoid <class-da-internal-prelude-monoid-6742_>`_ (`TextMap <type-da-internal-lf-textmap-11691_>`_ b)

  **instance** `Monoid <class-da-internal-prelude-monoid-6742_>`_ :ref:`All <type-da-monoid-types-all-38142>`

  **instance** `Monoid <class-da-internal-prelude-monoid-6742_>`_ :ref:`Any <type-da-monoid-types-any-3989>`

  **instance** `Monoid <class-da-internal-prelude-monoid-6742_>`_ (:ref:`Endo <type-da-monoid-types-endo-95420>` a)

  **instance** `Multiplicative <class-ghc-num-multiplicative-10593_>`_ a \=\> `Monoid <class-da-internal-prelude-monoid-6742_>`_ (:ref:`Product <type-da-monoid-types-product-66754>` a)

  **instance** `Additive <class-ghc-num-additive-25881_>`_ a \=\> `Monoid <class-da-internal-prelude-monoid-6742_>`_ (:ref:`Sum <type-da-monoid-types-sum-76394>` a)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ k \=\> `Monoid <class-da-internal-prelude-monoid-6742_>`_ (:ref:`Set <type-da-set-types-set-90436>` k)

  **instance** `Monoid <class-da-internal-prelude-monoid-6742_>`_ `Ordering <type-ghc-types-ordering-35353_>`_

  **instance** `Monoid <class-da-internal-prelude-monoid-6742_>`_ `Text <type-ghc-types-text-51952_>`_

  **instance** `Monoid <class-da-internal-prelude-monoid-6742_>`_ \[a\]

.. _class-da-internal-template-functions-hassignatory-17507:

**class** `HasSignatory <class-da-internal-template-functions-hassignatory-17507_>`_ t **where**

  Exposes ``signatory`` function\. Part of the ``Template`` constraint\.

  .. _function-da-internal-template-functions-signatory-70149:

  `signatory <function-da-internal-template-functions-signatory-70149_>`_
    \: t \-\> \[`Party <type-da-internal-lf-party-57932_>`_\]

    The signatories of a contract\.

.. _class-da-internal-template-functions-hasobserver-3182:

**class** `HasObserver <class-da-internal-template-functions-hasobserver-3182_>`_ t **where**

  Exposes ``observer`` function\. Part of the ``Template`` constraint\.

  .. _function-da-internal-template-functions-observer-97032:

  `observer <function-da-internal-template-functions-observer-97032_>`_
    \: t \-\> \[`Party <type-da-internal-lf-party-57932_>`_\]

    The observers of a contract\.

.. _class-da-internal-template-functions-hasensure-18132:

**class** `HasEnsure <class-da-internal-template-functions-hasensure-18132_>`_ t **where**

  Exposes ``ensure`` function\. Part of the ``Template`` constraint\.

  .. _function-da-internal-template-functions-ensure-45498:

  `ensure <function-da-internal-template-functions-ensure-45498_>`_
    \: t \-\> `Bool <type-ghc-types-bool-66265_>`_

    A predicate that must be true, otherwise contract creation will fail\.

.. _class-da-internal-template-functions-hascreate-45738:

**class** `HasCreate <class-da-internal-template-functions-hascreate-45738_>`_ t **where**

  Exposes ``create`` function\. Part of the ``Template`` constraint\.

  .. _function-da-internal-template-functions-create-34708:

  `create <function-da-internal-template-functions-create-34708_>`_
    \: t \-\> `Update <type-da-internal-lf-update-68072_>`_ (`ContractId <type-da-internal-lf-contractid-95282_>`_ t)

    Create a contract based on a template ``t``\.

.. _class-da-internal-template-functions-hasfetch-52387:

**class** `HasFetch <class-da-internal-template-functions-hasfetch-52387_>`_ t **where**

  Exposes ``fetch`` function\. Part of the ``Template`` constraint\.

  .. _function-da-internal-template-functions-fetch-90069:

  `fetch <function-da-internal-template-functions-fetch-90069_>`_
    \: `ContractId <type-da-internal-lf-contractid-95282_>`_ t \-\> `Update <type-da-internal-lf-update-68072_>`_ t

    Fetch the contract data associated with the given contract ID\.
    If the ``ContractId t`` supplied is not the contract ID of an active
    contract, this fails and aborts the entire transaction\.

.. _class-da-internal-template-functions-hassoftfetch-65731:

**class** `HasSoftFetch <class-da-internal-template-functions-hassoftfetch-65731_>`_ t **where**

  Exposes ``softFetch`` function

.. _class-da-internal-template-functions-hassoftexercise-29758:

**class** `HasSoftExercise <class-da-internal-template-functions-hassoftexercise-29758_>`_ t c r **where**


.. _class-da-internal-template-functions-hasarchive-7071:

**class** `HasArchive <class-da-internal-template-functions-hasarchive-7071_>`_ t **where**

  Exposes ``archive`` function\. Part of the ``Template`` constraint\.

  .. _function-da-internal-template-functions-archive-2977:

  `archive <function-da-internal-template-functions-archive-2977_>`_
    \: `ContractId <type-da-internal-lf-contractid-95282_>`_ t \-\> `Update <type-da-internal-lf-update-68072_>`_ ()

    Archive the contract with the given contract ID\.

.. _class-da-internal-template-functions-hastemplatetyperep-24134:

**class** `HasTemplateTypeRep <class-da-internal-template-functions-hastemplatetyperep-24134_>`_ t **where**

  Exposes ``templateTypeRep`` function in Daml\-LF 1\.7 or later\.
  Part of the ``Template`` constraint\.

.. _class-da-internal-template-functions-hastoanytemplate-94418:

**class** `HasToAnyTemplate <class-da-internal-template-functions-hastoanytemplate-94418_>`_ t **where**

  Exposes ``toAnyTemplate`` function in Daml\-LF 1\.7 or later\.
  Part of the ``Template`` constraint\.

.. _class-da-internal-template-functions-hasfromanytemplate-95481:

**class** `HasFromAnyTemplate <class-da-internal-template-functions-hasfromanytemplate-95481_>`_ t **where**

  Exposes ``fromAnyTemplate`` function in Daml\-LF 1\.7 or later\.
  Part of the ``Template`` constraint\.

.. _class-da-internal-template-functions-hasexercise-70422:

**class** `HasExercise <class-da-internal-template-functions-hasexercise-70422_>`_ t c r **where**

  Exposes ``exercise`` function\. Part of the ``Choice`` constraint\.

  .. _function-da-internal-template-functions-exercise-22396:

  `exercise <function-da-internal-template-functions-exercise-22396_>`_
    \: `ContractId <type-da-internal-lf-contractid-95282_>`_ t \-\> c \-\> `Update <type-da-internal-lf-update-68072_>`_ r

    Exercise a choice on the contract with the given contract ID\.

.. _class-da-internal-template-functions-haschoicecontroller-39229:

**class** `HasChoiceController <class-da-internal-template-functions-haschoicecontroller-39229_>`_ t c **where**

  Exposes ``choiceController`` function\. Part of the ``Choice`` constraint\.

.. _class-da-internal-template-functions-haschoiceobserver-31221:

**class** `HasChoiceObserver <class-da-internal-template-functions-haschoiceobserver-31221_>`_ t c **where**

  Exposes ``choiceObserver`` function\. Part of the ``Choice`` constraint\.

.. _class-da-internal-template-functions-hasexerciseguarded-97843:

**class** `HasExerciseGuarded <class-da-internal-template-functions-hasexerciseguarded-97843_>`_ t c r **where**

  (1\.dev only) Exposes ``exerciseGuarded`` function\.
  Only available for interface choices\.

  .. _function-da-internal-template-functions-exerciseguarded-9285:

  `exerciseGuarded <function-da-internal-template-functions-exerciseguarded-9285_>`_
    \: (t \-\> `Bool <type-ghc-types-bool-66265_>`_) \-\> `ContractId <type-da-internal-lf-contractid-95282_>`_ t \-\> c \-\> `Update <type-da-internal-lf-update-68072_>`_ r

    (1\.dev only) Exercise a choice on the contract with
    the given contract ID, only if the predicate returns ``True``\.

.. _class-da-internal-template-functions-hastoanychoice-82571:

**class** `HasToAnyChoice <class-da-internal-template-functions-hastoanychoice-82571_>`_ t c r **where**

  Exposes ``toAnyChoice`` function for Daml\-LF 1\.7 or later\.
  Part of the ``Choice`` constraint\.

.. _class-da-internal-template-functions-hasfromanychoice-81184:

**class** `HasFromAnyChoice <class-da-internal-template-functions-hasfromanychoice-81184_>`_ t c r **where**

  Exposes ``fromAnyChoice`` function for Daml\-LF 1\.7 or later\.
  Part of the ``Choice`` constraint\.

.. _class-da-internal-template-functions-haskey-87616:

**class** `HasKey <class-da-internal-template-functions-haskey-87616_>`_ t k **where**

  Exposes ``key`` function\. Part of the ``TemplateKey`` constraint\.

  .. _function-da-internal-template-functions-key-44978:

  `key <function-da-internal-template-functions-key-44978_>`_
    \: t \-\> k

    The key of a contract\.

.. _class-da-internal-template-functions-haslookupbykey-92299:

**class** `HasLookupByKey <class-da-internal-template-functions-haslookupbykey-92299_>`_ t k **where**

  Exposes ``lookupByKey`` function\. Part of the ``TemplateKey`` constraint\.

  .. _function-da-internal-template-functions-lookupbykey-92781:

  `lookupByKey <function-da-internal-template-functions-lookupbykey-92781_>`_
    \: k \-\> `Update <type-da-internal-lf-update-68072_>`_ (`Optional <type-da-internal-prelude-optional-37153_>`_ (`ContractId <type-da-internal-lf-contractid-95282_>`_ t))

    Look up the contract ID ``t`` associated with a given contract key ``k``\.

    You must pass the ``t`` using an explicit type application\. For
    instance, if you want to look up a contract of template ``Account`` by its
    key ``k``, you must call ``lookupByKey @Account k``\.

.. _class-da-internal-template-functions-hasfetchbykey-54638:

**class** `HasFetchByKey <class-da-internal-template-functions-hasfetchbykey-54638_>`_ t k **where**

  Exposes ``fetchByKey`` function\. Part of the ``TemplateKey`` constraint\.

  .. _function-da-internal-template-functions-fetchbykey-95464:

  `fetchByKey <function-da-internal-template-functions-fetchbykey-95464_>`_
    \: k \-\> `Update <type-da-internal-lf-update-68072_>`_ (`ContractId <type-da-internal-lf-contractid-95282_>`_ t, t)

    Fetch the contract ID and contract data associated with a given
    contract key\.

    You must pass the ``t`` using an explicit type application\. For
    instance, if you want to fetch a contract of template ``Account`` by its
    key ``k``, you must call ``fetchByKey @Account k``\.

.. _class-da-internal-template-functions-hasquerynbykey-53843:

**class** `HasQueryNByKey <class-da-internal-template-functions-hasquerynbykey-53843_>`_ t k **where**


.. _class-da-internal-template-functions-hasmaintainer-28932:

**class** `HasMaintainer <class-da-internal-template-functions-hasmaintainer-28932_>`_ t k **where**

  Exposes ``maintainer`` function\. Part of the ``TemplateKey`` constraint\.

.. _class-da-internal-template-functions-hastoanycontractkey-35010:

**class** `HasToAnyContractKey <class-da-internal-template-functions-hastoanycontractkey-35010_>`_ t k **where**

  Exposes ``toAnyContractKey`` function in Daml\-LF 1\.7 or later\.
  Part of the ``TemplateKey`` constraint\.

.. _class-da-internal-template-functions-hasfromanycontractkey-95587:

**class** `HasFromAnyContractKey <class-da-internal-template-functions-hasfromanycontractkey-95587_>`_ t k **where**

  Exposes ``fromAnyContractKey`` function in Daml\-LF 1\.7 or later\.
  Part of the ``TemplateKey`` constraint\.

.. _class-da-internal-template-functions-hasexercisebykey-36549:

**class** `HasExerciseByKey <class-da-internal-template-functions-hasexercisebykey-36549_>`_ t k c r **where**

  Exposes ``exerciseByKey`` function\.

.. _class-da-internal-template-functions-isparties-53750:

**class** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ a **where**

  Accepted ways to specify a list of parties\: either a single party, or a list of parties\.

  .. _function-da-internal-template-functions-toparties-75184:

  `toParties <function-da-internal-template-functions-toparties-75184_>`_
    \: a \-\> \[`Party <type-da-internal-lf-party-57932_>`_\]

    Convert to list of parties\.

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ `Party <type-da-internal-lf-party-57932_>`_

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ (`Optional <type-da-internal-prelude-optional-37153_>`_ `Party <type-da-internal-lf-party-57932_>`_)

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ (:ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` `Party <type-da-internal-lf-party-57932_>`_)

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ (:ref:`Set <type-da-set-types-set-90436>` `Party <type-da-internal-lf-party-57932_>`_)

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ \[`Party <type-da-internal-lf-party-57932_>`_\]

.. _class-ghc-base-functor-31205:

**class** `Functor <class-ghc-base-functor-31205_>`_ f **where**

  A ``Functor`` is a typeclass for things that can be mapped over (using
  its ``fmap`` function\. Examples include ``Optional``, ``[]`` and ``Update``)\.

  .. _function-ghc-base-fmap-51390:

  `fmap <function-ghc-base-fmap-51390_>`_
    \: (a \-\> b) \-\> f a \-\> f b

    ``fmap`` takes a function of type ``a -> b``, and turns it into a
    function of type ``f a -> f b``, where ``f`` is the type which is an
    instance of ``Functor``\.

    For example, ``map`` is an ``fmap`` that only works on lists\.
    It takes a function ``a -> b`` and a ``[a]``, and returns a ``[b]``\.

  .. _function-ghc-base-ltdollar-81052:

  `(<$) <function-ghc-base-ltdollar-81052_>`_
    \: a \-\> f b \-\> f a

    Replace all locations in the input ``f b`` with the same value ``a``\.
    The default definition is ``fmap . const``, but you can override
    this with a more efficient version\.

.. _class-ghc-classes-eq-22713:

**class** `Eq <class-ghc-classes-eq-22713_>`_ a **where**

  The ``Eq`` class defines equality (``==``) and inequality (``/=``)\.
  All the basic datatypes exported by the \"Prelude\" are instances of ``Eq``,
  and ``Eq`` may be derived for any datatype whose constituents are also
  instances of ``Eq``\.

  Usually, ``==`` is expected to implement an equivalence relationship where two
  values comparing equal are indistinguishable by \"public\" functions, with
  a \"public\" function being one not allowing to see implementation details\. For
  example, for a type representing non\-normalised natural numbers modulo 100,
  a \"public\" function doesn't make the difference between 1 and 201\. It is
  expected to have the following properties\:

  **Reflexivity**\: ``x == x`` \= ``True``

  **Symmetry**\: ``x == y`` \= ``y == x``

  **Transitivity**\: if ``x == y && y == z`` \= ``True``, then ``x == z`` \= ``True``

  **Substitutivity**\: if ``x == y`` \= ``True`` and ``f`` is a \"public\" function
  whose return type is an instance of ``Eq``, then ``f x == f y`` \= ``True``

  **Negation**\: ``x /= y`` \= ``not (x == y)``

  Minimal complete definition\: either ``==`` or ``/=``\.

  .. _function-ghc-classes-eqeq-39798:

  `(==) <function-ghc-classes-eqeq-39798_>`_
    \: a \-\> a \-\> `Bool <type-ghc-types-bool-66265_>`_

  .. _function-ghc-classes-slasheq-13204:

  `(/=) <function-ghc-classes-slasheq-13204_>`_
    \: a \-\> a \-\> `Bool <type-ghc-types-bool-66265_>`_

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (`Either <type-da-types-either-56020_>`_ a b)

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Bool <type-ghc-types-bool-66265_>`_

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Ordering <type-ghc-types-ordering-35353_>`_

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Text <type-ghc-types-text-51952_>`_

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ a \=\> `Eq <class-ghc-classes-eq-22713_>`_ \[a\]

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ ()

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e, `Eq <class-ghc-classes-eq-22713_>`_ f) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e, f)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e, `Eq <class-ghc-classes-eq-22713_>`_ f, `Eq <class-ghc-classes-eq-22713_>`_ g) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e, f, g)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e, `Eq <class-ghc-classes-eq-22713_>`_ f, `Eq <class-ghc-classes-eq-22713_>`_ g, `Eq <class-ghc-classes-eq-22713_>`_ h) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e, f, g, h)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e, `Eq <class-ghc-classes-eq-22713_>`_ f, `Eq <class-ghc-classes-eq-22713_>`_ g, `Eq <class-ghc-classes-eq-22713_>`_ h, `Eq <class-ghc-classes-eq-22713_>`_ i) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e, f, g, h, i)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e, `Eq <class-ghc-classes-eq-22713_>`_ f, `Eq <class-ghc-classes-eq-22713_>`_ g, `Eq <class-ghc-classes-eq-22713_>`_ h, `Eq <class-ghc-classes-eq-22713_>`_ i, `Eq <class-ghc-classes-eq-22713_>`_ j) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e, f, g, h, i, j)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e, `Eq <class-ghc-classes-eq-22713_>`_ f, `Eq <class-ghc-classes-eq-22713_>`_ g, `Eq <class-ghc-classes-eq-22713_>`_ h, `Eq <class-ghc-classes-eq-22713_>`_ i, `Eq <class-ghc-classes-eq-22713_>`_ j, `Eq <class-ghc-classes-eq-22713_>`_ k) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e, f, g, h, i, j, k)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e, `Eq <class-ghc-classes-eq-22713_>`_ f, `Eq <class-ghc-classes-eq-22713_>`_ g, `Eq <class-ghc-classes-eq-22713_>`_ h, `Eq <class-ghc-classes-eq-22713_>`_ i, `Eq <class-ghc-classes-eq-22713_>`_ j, `Eq <class-ghc-classes-eq-22713_>`_ k, `Eq <class-ghc-classes-eq-22713_>`_ l) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e, f, g, h, i, j, k, l)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e, `Eq <class-ghc-classes-eq-22713_>`_ f, `Eq <class-ghc-classes-eq-22713_>`_ g, `Eq <class-ghc-classes-eq-22713_>`_ h, `Eq <class-ghc-classes-eq-22713_>`_ i, `Eq <class-ghc-classes-eq-22713_>`_ j, `Eq <class-ghc-classes-eq-22713_>`_ k, `Eq <class-ghc-classes-eq-22713_>`_ l, `Eq <class-ghc-classes-eq-22713_>`_ m) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e, f, g, h, i, j, k, l, m)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e, `Eq <class-ghc-classes-eq-22713_>`_ f, `Eq <class-ghc-classes-eq-22713_>`_ g, `Eq <class-ghc-classes-eq-22713_>`_ h, `Eq <class-ghc-classes-eq-22713_>`_ i, `Eq <class-ghc-classes-eq-22713_>`_ j, `Eq <class-ghc-classes-eq-22713_>`_ k, `Eq <class-ghc-classes-eq-22713_>`_ l, `Eq <class-ghc-classes-eq-22713_>`_ m, `Eq <class-ghc-classes-eq-22713_>`_ n) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e, f, g, h, i, j, k, l, m, n)

  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b, `Eq <class-ghc-classes-eq-22713_>`_ c, `Eq <class-ghc-classes-eq-22713_>`_ d, `Eq <class-ghc-classes-eq-22713_>`_ e, `Eq <class-ghc-classes-eq-22713_>`_ f, `Eq <class-ghc-classes-eq-22713_>`_ g, `Eq <class-ghc-classes-eq-22713_>`_ h, `Eq <class-ghc-classes-eq-22713_>`_ i, `Eq <class-ghc-classes-eq-22713_>`_ j, `Eq <class-ghc-classes-eq-22713_>`_ k, `Eq <class-ghc-classes-eq-22713_>`_ l, `Eq <class-ghc-classes-eq-22713_>`_ m, `Eq <class-ghc-classes-eq-22713_>`_ n, `Eq <class-ghc-classes-eq-22713_>`_ o) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)

.. _class-ghc-classes-ord-6395:

**class** `Eq <class-ghc-classes-eq-22713_>`_ a \=\> `Ord <class-ghc-classes-ord-6395_>`_ a **where**

  The ``Ord`` class is used for totally ordered datatypes\.

  Instances of ``Ord`` can be derived for any user\-defined datatype whose
  constituent types are in ``Ord``\. The declared order of the constructors in
  the data declaration determines the ordering in derived ``Ord`` instances\. The
  ``Ordering`` datatype allows a single comparison to determine the precise
  ordering of two objects\.

  The Haskell Report defines no laws for ``Ord``\. However, ``<=`` is customarily
  expected to implement a non\-strict partial order and have the following
  properties\:

  **Transitivity**\: if ``x <= y && y <= z`` \= ``True``, then ``x <= z`` \= ``True``

  **Reflexivity**\: ``x <= x`` \= ``True``

  **Antisymmetry**\: if ``x <= y && y <= x`` \= ``True``, then ``x == y`` \= ``True``

  Note that the following operator interactions are expected to hold\:

  1. ``x >= y`` \= ``y <= x``
  2. ``x < y`` \= ``x <= y && x /= y``
  3. ``x > y`` \= ``y < x``
  4. ``x < y`` \= ``compare x y == LT``
  5. ``x > y`` \= ``compare x y == GT``
  6. ``x == y`` \= ``compare x y == EQ``
  7. ``min x y == if x <= y then x else y`` \= 'True'
  8. ``max x y == if x >= y then x else y`` \= 'True'

  Minimal complete definition\: either ``compare`` or ``<=``\.
  Using ``compare`` can be more efficient for complex types\.

  .. _function-ghc-classes-compare-51066:

  `compare <function-ghc-classes-compare-51066_>`_
    \: a \-\> a \-\> `Ordering <type-ghc-types-ordering-35353_>`_

  .. _function-ghc-classes-lt-18689:

  `(<) <function-ghc-classes-lt-18689_>`_
    \: a \-\> a \-\> `Bool <type-ghc-types-bool-66265_>`_

  .. _function-ghc-classes-lteq-90533:

  `(<=) <function-ghc-classes-lteq-90533_>`_
    \: a \-\> a \-\> `Bool <type-ghc-types-bool-66265_>`_

  .. _function-ghc-classes-gt-9999:

  `(>) <function-ghc-classes-gt-9999_>`_
    \: a \-\> a \-\> `Bool <type-ghc-types-bool-66265_>`_

  .. _function-ghc-classes-gteq-27019:

  `(>=) <function-ghc-classes-gteq-27019_>`_
    \: a \-\> a \-\> `Bool <type-ghc-types-bool-66265_>`_

  .. _function-ghc-classes-max-68325:

  `max <function-ghc-classes-max-68325_>`_
    \: a \-\> a \-\> a

  .. _function-ghc-classes-min-95471:

  `min <function-ghc-classes-min-95471_>`_
    \: a \-\> a \-\> a

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (`Either <type-da-types-either-56020_>`_ a b)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Bool <type-ghc-types-bool-66265_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Ordering <type-ghc-types-ordering-35353_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Text <type-ghc-types-text-51952_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ a \=\> `Ord <class-ghc-classes-ord-6395_>`_ \[a\]

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ ()

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e, `Ord <class-ghc-classes-ord-6395_>`_ f) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e, f)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e, `Ord <class-ghc-classes-ord-6395_>`_ f, `Ord <class-ghc-classes-ord-6395_>`_ g) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e, f, g)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e, `Ord <class-ghc-classes-ord-6395_>`_ f, `Ord <class-ghc-classes-ord-6395_>`_ g, `Ord <class-ghc-classes-ord-6395_>`_ h) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e, f, g, h)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e, `Ord <class-ghc-classes-ord-6395_>`_ f, `Ord <class-ghc-classes-ord-6395_>`_ g, `Ord <class-ghc-classes-ord-6395_>`_ h, `Ord <class-ghc-classes-ord-6395_>`_ i) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e, f, g, h, i)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e, `Ord <class-ghc-classes-ord-6395_>`_ f, `Ord <class-ghc-classes-ord-6395_>`_ g, `Ord <class-ghc-classes-ord-6395_>`_ h, `Ord <class-ghc-classes-ord-6395_>`_ i, `Ord <class-ghc-classes-ord-6395_>`_ j) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e, f, g, h, i, j)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e, `Ord <class-ghc-classes-ord-6395_>`_ f, `Ord <class-ghc-classes-ord-6395_>`_ g, `Ord <class-ghc-classes-ord-6395_>`_ h, `Ord <class-ghc-classes-ord-6395_>`_ i, `Ord <class-ghc-classes-ord-6395_>`_ j, `Ord <class-ghc-classes-ord-6395_>`_ k) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e, f, g, h, i, j, k)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e, `Ord <class-ghc-classes-ord-6395_>`_ f, `Ord <class-ghc-classes-ord-6395_>`_ g, `Ord <class-ghc-classes-ord-6395_>`_ h, `Ord <class-ghc-classes-ord-6395_>`_ i, `Ord <class-ghc-classes-ord-6395_>`_ j, `Ord <class-ghc-classes-ord-6395_>`_ k, `Ord <class-ghc-classes-ord-6395_>`_ l) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e, f, g, h, i, j, k, l)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e, `Ord <class-ghc-classes-ord-6395_>`_ f, `Ord <class-ghc-classes-ord-6395_>`_ g, `Ord <class-ghc-classes-ord-6395_>`_ h, `Ord <class-ghc-classes-ord-6395_>`_ i, `Ord <class-ghc-classes-ord-6395_>`_ j, `Ord <class-ghc-classes-ord-6395_>`_ k, `Ord <class-ghc-classes-ord-6395_>`_ l, `Ord <class-ghc-classes-ord-6395_>`_ m) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e, f, g, h, i, j, k, l, m)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e, `Ord <class-ghc-classes-ord-6395_>`_ f, `Ord <class-ghc-classes-ord-6395_>`_ g, `Ord <class-ghc-classes-ord-6395_>`_ h, `Ord <class-ghc-classes-ord-6395_>`_ i, `Ord <class-ghc-classes-ord-6395_>`_ j, `Ord <class-ghc-classes-ord-6395_>`_ k, `Ord <class-ghc-classes-ord-6395_>`_ l, `Ord <class-ghc-classes-ord-6395_>`_ m, `Ord <class-ghc-classes-ord-6395_>`_ n) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e, f, g, h, i, j, k, l, m, n)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b, `Ord <class-ghc-classes-ord-6395_>`_ c, `Ord <class-ghc-classes-ord-6395_>`_ d, `Ord <class-ghc-classes-ord-6395_>`_ e, `Ord <class-ghc-classes-ord-6395_>`_ f, `Ord <class-ghc-classes-ord-6395_>`_ g, `Ord <class-ghc-classes-ord-6395_>`_ h, `Ord <class-ghc-classes-ord-6395_>`_ i, `Ord <class-ghc-classes-ord-6395_>`_ j, `Ord <class-ghc-classes-ord-6395_>`_ k, `Ord <class-ghc-classes-ord-6395_>`_ l, `Ord <class-ghc-classes-ord-6395_>`_ m, `Ord <class-ghc-classes-ord-6395_>`_ n, `Ord <class-ghc-classes-ord-6395_>`_ o) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)

.. _class-ghc-classes-numericscale-83720:

**class** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n **where**

  Is this a valid scale for the ``Numeric`` type?

  This typeclass is used to prevent the creation of Numeric values
  with too large a scale\. The scale controls the number of digits available
  after the decimal point, and it must be between 0 and 37 inclusive\.

  Thus the only available instances of this typeclass are ``NumericScale 0``
  through ``NumericScale 37``\. This cannot be extended without additional
  compiler and runtime support\. You cannot implement a custom instance
  of this typeclass\.

  If you have an error message in your code of the form \"No instance for
  ``(NumericScale n)``\", this is probably caused by having a numeric literal
  whose scale cannot be inferred by the compiler\. You can usually fix this
  by adding a type signature to the definition, or annotating the numeric
  literal directly (for example, instead of writing ``3.14159`` you can write
  ``(3.14159 : Numeric 5)``)\.

  .. _function-ghc-classes-numericscale-22799:

  `numericScale <function-ghc-classes-numericscale-22799_>`_
    \: `Int <type-ghc-types-int-37261_>`_

    Get the scale of a ``Numeric`` as an integer\. For example,
    ``numericScale (3.14159 : Numeric 5)`` equals ``5``\.

  .. _function-ghc-classes-numericone-15415:

  `numericOne <function-ghc-classes-numericone-15415_>`_
    \: `Numeric <type-ghc-types-numeric-891_>`_ n

    1 with scale n

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 0

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 1

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 10

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 11

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 12

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 13

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 14

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 15

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 16

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 17

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 18

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 19

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 2

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 20

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 21

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 22

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 23

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 24

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 25

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 26

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 27

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 28

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 29

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 3

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 30

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 31

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 32

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 33

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 34

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 35

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 36

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 37

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 4

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 5

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 6

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 7

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 8

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ 9

.. _class-ghc-enum-bounded-34379:

**class** `Bounded <class-ghc-enum-bounded-34379_>`_ a **where**

  Use the ``Bounded`` class to name the upper and lower limits of a
  type\.

  You can derive an instance of the ``Bounded`` class for any enumeration
  type\. ``minBound``  is the first constructor listed in the ``data``
  declaration and ``maxBound`` is the last\.

  You can also derive an instance of ``Bounded`` for single\-constructor data types whose
  constituent types are in ``Bounded``\.

  ``Ord`` is not a superclass of ``Bounded`` because types that are not
  totally ordered can still have upper and lower bounds\.

  .. _function-ghc-enum-minbound-62730:

  `minBound <function-ghc-enum-minbound-62730_>`_
    \: a

  .. _function-ghc-enum-maxbound-99484:

  `maxBound <function-ghc-enum-maxbound-99484_>`_
    \: a

  **instance** `Bounded <class-ghc-enum-bounded-34379_>`_ `Bool <type-ghc-types-bool-66265_>`_

  **instance** `Bounded <class-ghc-enum-bounded-34379_>`_ `Int <type-ghc-types-int-37261_>`_

.. _class-ghc-enum-enum-63048:

**class** `Enum <class-ghc-enum-enum-63048_>`_ a **where**

  Use the ``Enum`` class to define operations on sequentially ordered
  types\: that is, types that can be enumerated\. ``Enum`` members have
  defined successors and predecessors, which you can get with the ``succ``
  and ``pred`` functions\.

  Types that are an instance of class ``Bounded`` as well as ``Enum``
  should respect the following laws\:

  * Both ``succ maxBound`` and ``pred minBound`` should result in
    a runtime error\.
  * ``fromEnum`` and ``toEnum`` should give a runtime error if the
    result value is not representable in the result type\.
    For example, ``toEnum 7 : Bool`` is an error\.
  * ``enumFrom`` and ``enumFromThen`` should be defined with an implicit bound,
    like this\:

  .. code-block:: daml

    enumFrom     x   = enumFromTo     x maxBound
    enumFromThen x y = enumFromThenTo x y bound
        where
            bound | fromEnum y >= fromEnum x = maxBound
                  | otherwise                = minBound

  .. _function-ghc-enum-succ-78724:

  `succ <function-ghc-enum-succ-78724_>`_
    \: a \-\> a

    Returns the successor of the given value\. For example, for
    numeric types, ``succ`` adds 1\.

    If the type is also an instance of ``Bounded``, ``succ maxBound``
    results in a runtime error\.

  .. _function-ghc-enum-pred-25539:

  `pred <function-ghc-enum-pred-25539_>`_
    \: a \-\> a

    Returns the predecessor of the given value\. For example, for
    numeric types, ``pred`` subtracts 1\.

    If the type is also an instance of ``Bounded``, ``pred minBound``
    results in a runtime error\.

  .. _function-ghc-enum-toenum-73120:

  `toEnum <function-ghc-enum-toenum-73120_>`_
    \: `Int <type-ghc-types-int-37261_>`_ \-\> a

    Convert a value from an ``Int`` to an ``Enum`` value\: ie,
    ``toEnum i`` returns the item at the ``i`` th position of
    (the instance of) ``Enum``

  .. _function-ghc-enum-fromenum-36901:

  `fromEnum <function-ghc-enum-fromenum-36901_>`_
    \: a \-\> `Int <type-ghc-types-int-37261_>`_

    Convert a value from an ``Enum`` value to an ``Int``\: ie, returns
    the ``Int`` position of the element within the ``Enum``\.

    If ``fromEnum`` is applied to a value that's too large to
    fit in an ``Int``, what is returned is up to your implementation\.

  .. _function-ghc-enum-enumfrom-64349:

  `enumFrom <function-ghc-enum-enumfrom-64349_>`_
    \: a \-\> \[a\]

    Return a list of the ``Enum`` values starting at the ``Int``
    position\. For example\:

    * ``enumFrom 6 : [Int] = [6,7,8,9,...,maxBound : Int]``

  .. _function-ghc-enum-enumfromthen-57624:

  `enumFromThen <function-ghc-enum-enumfromthen-57624_>`_
    \: a \-\> a \-\> \[a\]

    Returns a list of the ``Enum`` values with the first value at
    the first ``Int`` position, the second value at the second ``Int``
    position, and further values with the same distance between them\.

    For example\:

    * ``enumFromThen 4 6 : [Int] = [4,6,8,10...]``
    * ``enumFromThen 6 2 : [Int] = [6,2,-2,-6,...,minBound :: Int]``

  .. _function-ghc-enum-enumfromto-5096:

  `enumFromTo <function-ghc-enum-enumfromto-5096_>`_
    \: a \-\> a \-\> \[a\]

    Returns a list of the ``Enum`` values with the first value at
    the first ``Int`` position, and the last value at the last ``Int``
    position\.

    This is what's behind the language feature that lets you write
    ``[n,m..]``\.

    For example\:

    * ``enumFromTo 6 10 : [Int] = [6,7,8,9,10]``

  .. _function-ghc-enum-enumfromthento-6169:

  `enumFromThenTo <function-ghc-enum-enumfromthento-6169_>`_
    \: a \-\> a \-\> a \-\> \[a\]

    Returns a list of the ``Enum`` values with the first value at
    the first ``Int`` position, the second value at the second ``Int``
    position, and further values with the same distance between them,
    with the final value at the final ``Int`` position\.

    This is what's behind the language feature that lets you write
    ``[n,n'..m]``\.

    For example\:

    * ``enumFromThenTo 4 2 -6 : [Int] = [4,2,0,-2,-4,-6]``
    * ``enumFromThenTo 6 8 2 : [Int] = []``

  **instance** `Enum <class-ghc-enum-enum-63048_>`_ `Bool <type-ghc-types-bool-66265_>`_

  **instance** `Enum <class-ghc-enum-enum-63048_>`_ `Int <type-ghc-types-int-37261_>`_

.. _class-ghc-num-additive-25881:

**class** `Additive <class-ghc-num-additive-25881_>`_ a **where**

  Use the ``Additive`` class for types that can be added\.
  Instances have to respect the following laws\:

  * ``(+)`` must be associative, ie\: ``(x + y) + z`` \= ``x + (y + z)``
  * ``(+)`` must be commutative, ie\: ``x + y`` \= ``y + x``
  * ``x + aunit`` \= ``x``
  * ``negate`` gives the additive inverse, ie\: ``x + negate x`` \= ``aunit``

  .. _function-ghc-num-plus-27850:

  `(+) <function-ghc-num-plus-27850_>`_
    \: a \-\> a \-\> a

    Add the two arguments together\.

  .. _function-ghc-num-aunit-61822:

  `aunit <function-ghc-num-aunit-61822_>`_
    \: a

    The additive identity for the type\. For example, for numbers, this is 0\.

  .. _function-ghc-num-dash-19160:

  `(-) <function-ghc-num-dash-19160_>`_
    \: a \-\> a \-\> a

    Subtract the second argument from the first argument, ie\. ``x - y`` \= ``x + negate y``

  .. _function-ghc-num-negate-48522:

  `negate <function-ghc-num-negate-48522_>`_
    \: a \-\> a

    Negate the argument\: ``x + negate x`` \= ``aunit``

  **instance** `Additive <class-ghc-num-additive-25881_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Additive <class-ghc-num-additive-25881_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

.. _class-ghc-num-multiplicative-10593:

**class** `Multiplicative <class-ghc-num-multiplicative-10593_>`_ a **where**

  Use the ``Multiplicative`` class for types that can be multiplied\.
  Instances have to respect the following laws\:

  * ``(*)`` is associative, ie\:``(x * y) * z`` \= ``x * (y * z)``
  * ``(*)`` is commutative, ie\: ``x * y`` \= ``y * x``
  * ``x * munit`` \= ``x``

  .. _function-ghc-num-star-29927:

  `(*) <function-ghc-num-star-29927_>`_
    \: a \-\> a \-\> a

    Multipy the arguments together

  .. _function-ghc-num-munit-70418:

  `munit <function-ghc-num-munit-70418_>`_
    \: a

    The multiplicative identity for the type\. For example, for numbers, this is 1\.

  .. _function-ghc-num-hat-82067:

  `(^) <function-ghc-num-hat-82067_>`_
    \: a \-\> `Int <type-ghc-types-int-37261_>`_ \-\> a

    ``x ^ n`` raises ``x`` to the power of ``n``\.

  **instance** `Multiplicative <class-ghc-num-multiplicative-10593_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Multiplicative <class-ghc-num-multiplicative-10593_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

.. _class-ghc-num-number-53664:

**class** (`Additive <class-ghc-num-additive-25881_>`_ a, `Multiplicative <class-ghc-num-multiplicative-10593_>`_ a) \=\> `Number <class-ghc-num-number-53664_>`_ a **where**

  ``Number`` is a class for numerical types\.
  As well as the rules for ``Additive`` and ``Multiplicative``, instances
  also have to respect the following law\:

  * ``(*)`` is distributive with respect to ``(+)``\. That is\:
    ``a * (b + c)`` \= ``(a * b) + (a * c)`` and ``(b + c) * a`` \= ``(b * a) + (c * a)``

  **instance** `Number <class-ghc-num-number-53664_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Number <class-ghc-num-number-53664_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

.. _class-ghc-num-signed-2671:

**class** `Signed <class-ghc-num-signed-2671_>`_ a **where**

  The ``Signed`` is for the sign of a number\.

  .. _function-ghc-num-signum-92649:

  `signum <function-ghc-num-signum-92649_>`_
    \: a \-\> a

    Sign of a number\.
    For real numbers, the 'signum' is either ``-1`` (negative), ``0`` (zero)
    or ``1`` (positive)\.

  .. _function-ghc-num-abs-35083:

  `abs <function-ghc-num-abs-35083_>`_
    \: a \-\> a

    The absolute value\: that is, the value without the sign\.

  **instance** `Signed <class-ghc-num-signed-2671_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Signed <class-ghc-num-signed-2671_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

.. _class-ghc-num-divisible-86689:

**class** `Multiplicative <class-ghc-num-multiplicative-10593_>`_ a \=\> `Divisible <class-ghc-num-divisible-86689_>`_ a **where**

  Use the ``Divisible`` class for types that can be divided\.
  Instances should respect that division is the inverse of
  multiplication, i\.e\. ``x * y / y`` is equal to ``x`` whenever
  it is defined\.

  .. _function-ghc-num-slash-10470:

  `(/) <function-ghc-num-slash-10470_>`_
    \: a \-\> a \-\> a

    ``x / y`` divides ``x`` by ``y``

  **instance** `Divisible <class-ghc-num-divisible-86689_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Divisible <class-ghc-num-divisible-86689_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

.. _class-ghc-num-fractional-79050:

**class** `Divisible <class-ghc-num-divisible-86689_>`_ a \=\> `Fractional <class-ghc-num-fractional-79050_>`_ a **where**

  Use the ``Fractional`` class for types that can be divided
  and where the reciprocal is well defined\. Instances
  have to respect the following laws\:

  * When ``recip x`` is defined, it must be the inverse of
    ``x`` with respect to multiplication\: ``x * recip x = munit``
  * When ``recip y`` is defined, then ``x / y = x * recip y``

  .. _function-ghc-num-recip-73404:

  `recip <function-ghc-num-recip-73404_>`_
    \: a \-\> a

    Calculates the reciprocal\: ``recip x`` is ``1/x``\.

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Fractional <class-ghc-num-fractional-79050_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

.. _class-ghc-show-show-65360:

**class** `Show <class-ghc-show-show-65360_>`_ a **where**

  Use the ``Show`` class for values that can be converted to a
  readable ``Text`` value\.

  Derived instances of ``Show`` have the following properties\:

  * The result of ``show`` is a syntactically correct expression
    that only contains constants (given the fixity declarations in
    force at the point where the type is declared)\.
    It only contains the constructor names defined in the data type,
    parentheses, and spaces\. When labelled constructor fields are
    used, braces, commas, field names, and equal signs are also used\.
  * If the constructor is defined to be an infix operator, then
    ``showsPrec`` produces infix applications of the constructor\.
  * If the precedence of the top\-level constructor in ``x`` is less than ``d``
    (associativity is ignored), the representation will be enclosed in
    parentheses\. For example, if ``d`` is ``0`` then the result
    is never surrounded in parentheses; if ``d`` is ``11`` it is always
    surrounded in parentheses, unless it is an atomic expression\.
  * If the constructor is defined using record syntax, then ``show``
    will produce the record\-syntax form, with the fields given in the
    same order as the original declaration\.

  .. _function-ghc-show-showsprec-34581:

  `showsPrec <function-ghc-show-showsprec-34581_>`_
    \: `Int <type-ghc-types-int-37261_>`_ \-\> a \-\> `ShowS <type-ghc-show-shows-46771_>`_

    Convert a value to a readable ``Text`` value\. Unlike ``show``,
    ``showsPrec`` should satisfy the rule
    ``showsPrec d x r ++ s == showsPrec d x (r ++ s)``

  .. _function-ghc-show-show-51173:

  `show <function-ghc-show-show-51173_>`_
    \: a \-\> `Text <type-ghc-types-text-51952_>`_

    Convert a value to a readable ``Text`` value\.

  .. _function-ghc-show-showlist-14969:

  `showList <function-ghc-show-showlist-14969_>`_
    \: \[a\] \-\> `ShowS <type-ghc-show-shows-46771_>`_

    Allows you to show lists of values\.

  **instance** (`Show <class-ghc-show-show-65360_>`_ a, `Show <class-ghc-show-show-65360_>`_ b) \=\> `Show <class-ghc-show-show-65360_>`_ (`Either <type-da-types-either-56020_>`_ a b)

  **instance** `Show <class-ghc-show-show-65360_>`_ `Bool <type-ghc-types-bool-66265_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `Show <class-ghc-show-show-65360_>`_ `Ordering <type-ghc-types-ordering-35353_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ `Text <type-ghc-types-text-51952_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ a \=\> `Show <class-ghc-show-show-65360_>`_ \[a\]

  **instance** `Show <class-ghc-show-show-65360_>`_ ()

  **instance** (`Show <class-ghc-show-show-65360_>`_ a, `Show <class-ghc-show-show-65360_>`_ b) \=\> `Show <class-ghc-show-show-65360_>`_ (a, b)

  **instance** (`Show <class-ghc-show-show-65360_>`_ a, `Show <class-ghc-show-show-65360_>`_ b, `Show <class-ghc-show-show-65360_>`_ c) \=\> `Show <class-ghc-show-show-65360_>`_ (a, b, c)

  **instance** (`Show <class-ghc-show-show-65360_>`_ a, `Show <class-ghc-show-show-65360_>`_ b, `Show <class-ghc-show-show-65360_>`_ c, `Show <class-ghc-show-show-65360_>`_ d) \=\> `Show <class-ghc-show-show-65360_>`_ (a, b, c, d)

  **instance** (`Show <class-ghc-show-show-65360_>`_ a, `Show <class-ghc-show-show-65360_>`_ b, `Show <class-ghc-show-show-65360_>`_ c, `Show <class-ghc-show-show-65360_>`_ d, `Show <class-ghc-show-show-65360_>`_ e) \=\> `Show <class-ghc-show-show-65360_>`_ (a, b, c, d, e)

Data Types
----------

.. _type-da-internal-any-anychoice-86490:

**data** `AnyChoice <type-da-internal-any-anychoice-86490_>`_

  Existential choice type that can wrap an arbitrary choice\.

  .. _constr-da-internal-any-anychoice-64121:

  `AnyChoice <constr-da-internal-any-anychoice-64121_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getAnyChoice
         - Any
         -
       * - getAnyChoiceTemplateTypeRep
         - `TemplateTypeRep <type-da-internal-any-templatetyperep-33792_>`_
         -

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `AnyChoice <type-da-internal-any-anychoice-86490_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `AnyChoice <type-da-internal-any-anychoice-86490_>`_

.. _type-da-internal-any-anycontractkey-68193:

**data** `AnyContractKey <type-da-internal-any-anycontractkey-68193_>`_

  Existential contract key type that can wrap an arbitrary contract key\.

  .. _constr-da-internal-any-anycontractkey-82848:

  `AnyContractKey <constr-da-internal-any-anycontractkey-82848_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getAnyContractKey
         - Any
         -
       * - getAnyContractKeyTemplateTypeRep
         - `TemplateTypeRep <type-da-internal-any-templatetyperep-33792_>`_
         -

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `AnyContractKey <type-da-internal-any-anycontractkey-68193_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `AnyContractKey <type-da-internal-any-anycontractkey-68193_>`_

.. _type-da-internal-any-anytemplate-63703:

**data** `AnyTemplate <type-da-internal-any-anytemplate-63703_>`_

  Existential template type that can wrap an arbitrary template\.

  .. _constr-da-internal-any-anytemplate-32540:

  `AnyTemplate <constr-da-internal-any-anytemplate-32540_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getAnyTemplate
         - Any
         -

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `AnyTemplate <type-da-internal-any-anytemplate-63703_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `AnyTemplate <type-da-internal-any-anytemplate-63703_>`_

.. _type-da-internal-any-templatetyperep-33792:

**data** `TemplateTypeRep <type-da-internal-any-templatetyperep-33792_>`_

  Unique textual representation of a template Id\.

  .. _constr-da-internal-any-templatetyperep-57303:

  `TemplateTypeRep <constr-da-internal-any-templatetyperep-57303_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getTemplateTypeRep
         - TypeRep
         -

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `TemplateTypeRep <type-da-internal-any-templatetyperep-33792_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `TemplateTypeRep <type-da-internal-any-templatetyperep-33792_>`_

.. _type-da-internal-down-down-61433:

**data** `Down <type-da-internal-down-down-61433_>`_ a

  The ``Down`` type can be used for reversing sorting order\.
  For example, ``sortOn (\x -> Down x.field)`` would sort by descending ``field``\.

  .. _constr-da-internal-down-down-26630:

  `Down <constr-da-internal-down-down-26630_>`_ a


  **instance** `Action <class-da-internal-prelude-action-68790_>`_ `Down <type-da-internal-down-down-61433_>`_

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ `Down <type-da-internal-down-down-61433_>`_

  **instance** `Functor <class-ghc-base-functor-31205_>`_ `Down <type-da-internal-down-down-61433_>`_

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ a \=\> `Eq <class-ghc-classes-eq-22713_>`_ (`Down <type-da-internal-down-down-61433_>`_ a)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ a \=\> `Ord <class-ghc-classes-ord-6395_>`_ (`Down <type-da-internal-down-down-61433_>`_ a)

  **instance** `Show <class-ghc-show-show-65360_>`_ a \=\> `Show <class-ghc-show-show-65360_>`_ (`Down <type-da-internal-down-down-61433_>`_ a)

.. _type-da-internal-interface-implements-92077:

**type** `Implements <type-da-internal-interface-implements-92077_>`_ t i
  \= (`HasInterfaceTypeRep <class-da-internal-interface-hasinterfacetyperep-84221_>`_ i, `HasToInterface <class-da-internal-interface-hastointerface-68104_>`_ t i, `HasFromInterface <class-da-internal-interface-hasfrominterface-43863_>`_ t i)

  (Daml\-LF \>\= 1\.15) Constraint that indicates that a template implements an interface\.

.. _type-da-internal-lf-anyexception-7004:

**data** `AnyException <type-da-internal-lf-anyexception-7004_>`_

  .. warning::
    **DEPRECATED**\:

    | Exceptions are deprecated, prefer ``failWithStatus``, and avoid using catch\.
    | Use ``-Wno-deprecated-exceptions`` to disable this warning\.

  A wrapper for all exception types\.

  **instance** :ref:`HasFromAnyException <class-da-internal-exception-hasfromanyexception-16788>` `AnyException <type-da-internal-lf-anyexception-7004_>`_

  **instance** :ref:`HasMessage <class-da-internal-exception-hasmessage-3179>` `AnyException <type-da-internal-lf-anyexception-7004_>`_

  **instance** :ref:`HasToAnyException <class-da-internal-exception-hastoanyexception-55973>` `AnyException <type-da-internal-lf-anyexception-7004_>`_

.. _type-da-internal-lf-contractid-95282:

**data** `ContractId <type-da-internal-lf-contractid-95282_>`_ a

  The ``ContractId a`` type represents an ID for a contract created from a template ``a``\.
  You can use the ID to fetch the contract, among other things\.

  **instance** Serializable (`ContractId <type-da-internal-lf-contractid-95282_>`_ a)

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ (`ContractId <type-da-internal-lf-contractid-95282_>`_ a)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ (`ContractId <type-da-internal-lf-contractid-95282_>`_ a)

  **instance** `Show <class-ghc-show-show-65360_>`_ (`ContractId <type-da-internal-lf-contractid-95282_>`_ a)

.. _type-da-internal-lf-date-32253:

**data** `Date <type-da-internal-lf-date-32253_>`_

  The ``Date`` type represents a date, for example ``date 2007 Apr 5``\.
  The bounds for Date are 0001\-01\-01 and 9999\-12\-31\.

  **instance** Serializable `Date <type-da-internal-lf-date-32253_>`_

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Date <type-da-internal-lf-date-32253_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Date <type-da-internal-lf-date-32253_>`_

  **instance** `Bounded <class-ghc-enum-bounded-34379_>`_ `Date <type-da-internal-lf-date-32253_>`_

  **instance** `Enum <class-ghc-enum-enum-63048_>`_ `Date <type-da-internal-lf-date-32253_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ `Date <type-da-internal-lf-date-32253_>`_

.. _type-da-internal-lf-map-90052:

**data** `Map <type-da-internal-lf-map-90052_>`_ a b

  The ``Map a b`` type represents an associative array from keys of type ``a``
  to values of type ``b``\. It uses the built\-in equality for keys\. Import
  ``DA.Map`` to use it\.

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ k \=\> :ref:`Foldable <class-da-foldable-foldable-25994>` (`Map <type-da-internal-lf-map-90052_>`_ k)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ k \=\> `Monoid <class-da-internal-prelude-monoid-6742_>`_ (`Map <type-da-internal-lf-map-90052_>`_ k v)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ k \=\> `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (`Map <type-da-internal-lf-map-90052_>`_ k v)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ k \=\> :ref:`Traversable <class-da-traversable-traversable-18144>` (`Map <type-da-internal-lf-map-90052_>`_ k)

  **instance** (Serializable a, Serializable b) \=\> Serializable (`Map <type-da-internal-lf-map-90052_>`_ a b)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ k \=\> `Functor <class-ghc-base-functor-31205_>`_ (`Map <type-da-internal-lf-map-90052_>`_ k)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ k, `Eq <class-ghc-classes-eq-22713_>`_ v) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (`Map <type-da-internal-lf-map-90052_>`_ k v)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ k, `Ord <class-ghc-classes-ord-6395_>`_ v) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (`Map <type-da-internal-lf-map-90052_>`_ k v)

  **instance** (`Show <class-ghc-show-show-65360_>`_ k, `Show <class-ghc-show-show-65360_>`_ v) \=\> `Show <class-ghc-show-show-65360_>`_ (`Map <type-da-internal-lf-map-90052_>`_ k v)

.. _type-da-internal-lf-party-57932:

**data** `Party <type-da-internal-lf-party-57932_>`_

  The ``Party`` type represents a party to a contract\.

  **instance** :ref:`HasFromHex <class-da-crypto-text-hasfromhex-84972>` (`Optional <type-da-internal-prelude-optional-37153_>`_ `Party <type-da-internal-lf-party-57932_>`_)

  **instance** :ref:`HasToHex <class-da-crypto-text-hastohex-92431>` `Party <type-da-internal-lf-party-57932_>`_

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ `Party <type-da-internal-lf-party-57932_>`_

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ (`Optional <type-da-internal-prelude-optional-37153_>`_ `Party <type-da-internal-lf-party-57932_>`_)

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ (:ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` `Party <type-da-internal-lf-party-57932_>`_)

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ (:ref:`Set <type-da-set-types-set-90436>` `Party <type-da-internal-lf-party-57932_>`_)

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ \[`Party <type-da-internal-lf-party-57932_>`_\]

  **instance** Serializable `Party <type-da-internal-lf-party-57932_>`_

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Party <type-da-internal-lf-party-57932_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Party <type-da-internal-lf-party-57932_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ `Party <type-da-internal-lf-party-57932_>`_

.. _type-da-internal-lf-textmap-11691:

**data** `TextMap <type-da-internal-lf-textmap-11691_>`_ a

  The ``TextMap a`` type represents an associative array from keys of type
  ``Text`` to values of type ``a``\.

  **instance** :ref:`Foldable <class-da-foldable-foldable-25994>` `TextMap <type-da-internal-lf-textmap-11691_>`_

  **instance** `Monoid <class-da-internal-prelude-monoid-6742_>`_ (`TextMap <type-da-internal-lf-textmap-11691_>`_ b)

  **instance** `Semigroup <class-da-internal-prelude-semigroup-78998_>`_ (`TextMap <type-da-internal-lf-textmap-11691_>`_ b)

  **instance** :ref:`Traversable <class-da-traversable-traversable-18144>` `TextMap <type-da-internal-lf-textmap-11691_>`_

  **instance** Serializable a \=\> Serializable (`TextMap <type-da-internal-lf-textmap-11691_>`_ a)

  **instance** `Functor <class-ghc-base-functor-31205_>`_ `TextMap <type-da-internal-lf-textmap-11691_>`_

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ a \=\> `Eq <class-ghc-classes-eq-22713_>`_ (`TextMap <type-da-internal-lf-textmap-11691_>`_ a)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ a \=\> `Ord <class-ghc-classes-ord-6395_>`_ (`TextMap <type-da-internal-lf-textmap-11691_>`_ a)

  **instance** `Show <class-ghc-show-show-65360_>`_ a \=\> `Show <class-ghc-show-show-65360_>`_ (`TextMap <type-da-internal-lf-textmap-11691_>`_ a)

.. _type-da-internal-lf-time-63886:

**data** `Time <type-da-internal-lf-time-63886_>`_

  The ``Time`` type represents a specific datetime in UTC,
  for example ``time (date 2007 Apr 5) 14 30 05``\.
  The bounds for Time are 0001\-01\-01T00\:00\:00\.000000Z and
  9999\-12\-31T23\:59\:59\.999999Z\.

  **instance** Serializable `Time <type-da-internal-lf-time-63886_>`_

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Time <type-da-internal-lf-time-63886_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Time <type-da-internal-lf-time-63886_>`_

  **instance** `Bounded <class-ghc-enum-bounded-34379_>`_ `Time <type-da-internal-lf-time-63886_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ `Time <type-da-internal-lf-time-63886_>`_

.. _type-da-internal-lf-update-68072:

**data** `Update <type-da-internal-lf-update-68072_>`_ a

  The ``Update a`` type represents an ``Action`` to update or query the ledger,
  before returning a value of type ``a``\. Examples include ``create`` and ``fetch``\.

  **instance** `CanAssert <class-da-internal-assert-canassert-67323_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** :ref:`ActionCatch <class-da-internal-exception-actioncatch-69238>` `Update <type-da-internal-lf-update-68072_>`_

  **instance** :ref:`ActionThrow <class-da-internal-exception-actionthrow-37623>` `Update <type-da-internal-lf-update-68072_>`_

  **instance** :ref:`ActionFailWithStatus <class-da-internal-fail-actionfailwithstatus-58664>` `Update <type-da-internal-lf-update-68072_>`_

  **instance** `CanAbort <class-da-internal-lf-canabort-29060_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** `HasTime <class-da-internal-lf-hastime-96546_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** `ActionFail <class-da-internal-prelude-actionfail-34438_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ `Update <type-da-internal-lf-update-68072_>`_

  **instance** `Functor <class-ghc-base-functor-31205_>`_ `Update <type-da-internal-lf-update-68072_>`_

.. _type-da-internal-prelude-optional-37153:

**data** `Optional <type-da-internal-prelude-optional-37153_>`_ a

  The ``Optional`` type encapsulates an optional value\. A value of type
  ``Optional a`` either contains a value of type ``a`` (represented as ``Some a``),
  or it is empty (represented as ``None``)\. Using ``Optional`` is a good way to
  deal with errors or exceptional cases without resorting to drastic
  measures such as ``error``\.

  The ``Optional`` type is also an ``Action``\. It is a simple kind of error
  ``Action``, where all errors are represented by ``None``\. A richer
  error ``Action`` could be built using the ``Data.Either.Either`` type\.

  .. _constr-da-internal-prelude-none-34906:

  `None <constr-da-internal-prelude-none-34906_>`_


  .. _constr-da-internal-prelude-some-71164:

  `Some <constr-da-internal-prelude-some-71164_>`_ a


  **instance** :ref:`HasFromHex <class-da-crypto-text-hasfromhex-84972>` (`Optional <type-da-internal-prelude-optional-37153_>`_ `Party <type-da-internal-lf-party-57932_>`_)

  **instance** :ref:`HasFromHex <class-da-crypto-text-hasfromhex-84972>` (`Optional <type-da-internal-prelude-optional-37153_>`_ `Int <type-ghc-types-int-37261_>`_)

  **instance** :ref:`HasFromHex <class-da-crypto-text-hasfromhex-84972>` (`Optional <type-da-internal-prelude-optional-37153_>`_ `Text <type-ghc-types-text-51952_>`_)

  **instance** :ref:`Foldable <class-da-foldable-foldable-25994>` `Optional <type-da-internal-prelude-optional-37153_>`_

  **instance** `Action <class-da-internal-prelude-action-68790_>`_ `Optional <type-da-internal-prelude-optional-37153_>`_

  **instance** `ActionFail <class-da-internal-prelude-actionfail-34438_>`_ `Optional <type-da-internal-prelude-optional-37153_>`_

  **instance** `Applicative <class-da-internal-prelude-applicative-9257_>`_ `Optional <type-da-internal-prelude-optional-37153_>`_

  **instance** `IsParties <class-da-internal-template-functions-isparties-53750_>`_ (`Optional <type-da-internal-prelude-optional-37153_>`_ `Party <type-da-internal-lf-party-57932_>`_)

  **instance** :ref:`Traversable <class-da-traversable-traversable-18144>` `Optional <type-da-internal-prelude-optional-37153_>`_

  **instance** Serializable a \=\> Serializable (`Optional <type-da-internal-prelude-optional-37153_>`_ a)

  **instance** `Functor <class-ghc-base-functor-31205_>`_ `Optional <type-da-internal-prelude-optional-37153_>`_

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ a \=\> `Eq <class-ghc-classes-eq-22713_>`_ (`Optional <type-da-internal-prelude-optional-37153_>`_ a)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ a \=\> `Ord <class-ghc-classes-ord-6395_>`_ (`Optional <type-da-internal-prelude-optional-37153_>`_ a)

  **instance** `Show <class-ghc-show-show-65360_>`_ a \=\> `Show <class-ghc-show-show-65360_>`_ (`Optional <type-da-internal-prelude-optional-37153_>`_ a)

.. _type-da-internal-template-archive-15178:

**data** `Archive <type-da-internal-template-archive-15178_>`_

  The data type corresponding to the implicit ``Archive``
  choice in every template\.

  .. _constr-da-internal-template-archive-55291:

  `Archive <constr-da-internal-template-archive-55291_>`_

    (no fields)

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Archive <type-da-internal-template-archive-15178_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ `Archive <type-da-internal-template-archive-15178_>`_

.. _type-da-internal-template-functions-choice-82157:

**type** `Choice <type-da-internal-template-functions-choice-82157_>`_ t c r
  \= (`Template <type-da-internal-template-functions-template-31804_>`_ t, `HasExercise <class-da-internal-template-functions-hasexercise-70422_>`_ t c r, `HasToAnyChoice <class-da-internal-template-functions-hastoanychoice-82571_>`_ t c r, `HasFromAnyChoice <class-da-internal-template-functions-hasfromanychoice-81184_>`_ t c r)

  Constraint satisfied by choices\.

.. _type-da-internal-template-functions-template-31804:

**type** `Template <type-da-internal-template-functions-template-31804_>`_ t
  \= (`HasTemplateTypeRep <class-da-internal-template-functions-hastemplatetyperep-24134_>`_ t, `HasToAnyTemplate <class-da-internal-template-functions-hastoanytemplate-94418_>`_ t, `HasFromAnyTemplate <class-da-internal-template-functions-hasfromanytemplate-95481_>`_ t)

.. _type-da-internal-template-functions-templatekey-95200:

**type** `TemplateKey <type-da-internal-template-functions-templatekey-95200_>`_ t k
  \= (`Template <type-da-internal-template-functions-template-31804_>`_ t, `HasKey <class-da-internal-template-functions-haskey-87616_>`_ t k, `HasLookupByKey <class-da-internal-template-functions-haslookupbykey-92299_>`_ t k, `HasFetchByKey <class-da-internal-template-functions-hasfetchbykey-54638_>`_ t k, `HasMaintainer <class-da-internal-template-functions-hasmaintainer-28932_>`_ t k, `HasToAnyContractKey <class-da-internal-template-functions-hastoanycontractkey-35010_>`_ t k, `HasFromAnyContractKey <class-da-internal-template-functions-hasfromanycontractkey-95587_>`_ t k)

  Constraint satisfied by template keys\.

.. _type-da-types-either-56020:

**data** `Either <type-da-types-either-56020_>`_ a b

  The ``Either`` type represents values with two possibilities\: a value of
  type ``Either a b`` is either ``Left a`` or ``Right b``\.

  The ``Either`` type is sometimes used to represent a value which is
  either correct or an error; by convention, the ``Left`` constructor is
  used to hold an error value and the ``Right`` constructor is used to
  hold a correct value (mnemonic\: \"right\" also means \"correct\")\.

  .. _constr-da-types-left-53933:

  `Left <constr-da-types-left-53933_>`_ a


  .. _constr-da-types-right-18483:

  `Right <constr-da-types-right-18483_>`_ b


  **instance** (`Eq <class-ghc-classes-eq-22713_>`_ a, `Eq <class-ghc-classes-eq-22713_>`_ b) \=\> `Eq <class-ghc-classes-eq-22713_>`_ (`Either <type-da-types-either-56020_>`_ a b)

  **instance** (`Ord <class-ghc-classes-ord-6395_>`_ a, `Ord <class-ghc-classes-ord-6395_>`_ b) \=\> `Ord <class-ghc-classes-ord-6395_>`_ (`Either <type-da-types-either-56020_>`_ a b)

  **instance** (`Show <class-ghc-show-show-65360_>`_ a, `Show <class-ghc-show-show-65360_>`_ b) \=\> `Show <class-ghc-show-show-65360_>`_ (`Either <type-da-types-either-56020_>`_ a b)

.. _type-ghc-show-shows-46771:

**type** `ShowS <type-ghc-show-shows-46771_>`_
  \= `Text <type-ghc-types-text-51952_>`_ \-\> `Text <type-ghc-types-text-51952_>`_

  ``showS`` should represent some text, and applying it to some argument
  should prepend the argument to the represented text\.

.. _type-ghc-types-bool-66265:

**data** `Bool <type-ghc-types-bool-66265_>`_

  A type for Boolean values, ie ``True`` and ``False``\.

  .. _constr-ghc-types-false-77590:

  `False <constr-ghc-types-false-77590_>`_


  .. _constr-ghc-types-true-99264:

  `True <constr-ghc-types-true-99264_>`_


  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Bool <type-ghc-types-bool-66265_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Bool <type-ghc-types-bool-66265_>`_

  **instance** `Bounded <class-ghc-enum-bounded-34379_>`_ `Bool <type-ghc-types-bool-66265_>`_

  **instance** `Enum <class-ghc-enum-enum-63048_>`_ `Bool <type-ghc-types-bool-66265_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ `Bool <type-ghc-types-bool-66265_>`_

.. _type-ghc-types-decimal-18135:

**type** `Decimal <type-ghc-types-decimal-18135_>`_
  \= `Numeric <type-ghc-types-numeric-891_>`_ 10

.. _type-ghc-types-int-37261:

**data** `Int <type-ghc-types-int-37261_>`_

  A type representing a 64\-bit integer\.

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Bounded <class-ghc-enum-bounded-34379_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Enum <class-ghc-enum-enum-63048_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Additive <class-ghc-num-additive-25881_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Divisible <class-ghc-num-divisible-86689_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Multiplicative <class-ghc-num-multiplicative-10593_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Number <class-ghc-num-number-53664_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Signed <class-ghc-num-signed-2671_>`_ `Int <type-ghc-types-int-37261_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ `Int <type-ghc-types-int-37261_>`_

.. _type-ghc-types-nat-55875:

**data** `Nat <type-ghc-types-nat-55875_>`_

  (Kind) This is the kind of type\-level naturals\.

.. _type-ghc-types-numeric-891:

**data** `Numeric <type-ghc-types-numeric-891_>`_ n

  A type for fixed\-point decimal numbers, with the scale
  being passed as part of the type\.

  ``Numeric n`` represents a fixed\-point decimal number with a
  fixed precision of 38 (i\.e\. 38 digits not including a leading zero)
  and a scale of ``n``, i\.e\., ``n`` digits after the decimal point\.

  ``n`` must be between 0 and 37 (bounds inclusive)\.

  Examples\:

  .. code-block:: daml

    0.01 : Numeric 2
    0.0001 : Numeric 4

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Additive <class-ghc-num-additive-25881_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Divisible <class-ghc-num-divisible-86689_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Fractional <class-ghc-num-fractional-79050_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Multiplicative <class-ghc-num-multiplicative-10593_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Number <class-ghc-num-number-53664_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Signed <class-ghc-num-signed-2671_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

  **instance** `Show <class-ghc-show-show-65360_>`_ (`Numeric <type-ghc-types-numeric-891_>`_ n)

.. _type-ghc-types-ordering-35353:

**data** `Ordering <type-ghc-types-ordering-35353_>`_

  A type for giving information about ordering\:
  being less than (``LT``), equal to (``EQ``), or greater than
  (``GT``) something\.

  .. _constr-ghc-types-lt-57618:

  `LT <constr-ghc-types-lt-57618_>`_


  .. _constr-ghc-types-eq-5100:

  `EQ <constr-ghc-types-eq-5100_>`_


  .. _constr-ghc-types-gt-28015:

  `GT <constr-ghc-types-gt-28015_>`_


  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Ordering <type-ghc-types-ordering-35353_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Ordering <type-ghc-types-ordering-35353_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ `Ordering <type-ghc-types-ordering-35353_>`_

.. _type-ghc-types-text-51952:

**data** `Text <type-ghc-types-text-51952_>`_

  A type for text strings, that can represent any unicode code point\.
  For example ``"Hello, world"``\.

  **instance** `Eq <class-ghc-classes-eq-22713_>`_ `Text <type-ghc-types-text-51952_>`_

  **instance** `Ord <class-ghc-classes-ord-6395_>`_ `Text <type-ghc-types-text-51952_>`_

  **instance** `Show <class-ghc-show-show-65360_>`_ `Text <type-ghc-types-text-51952_>`_

.. _type-ghc-types-x-2599:

**data** `[] <type-ghc-types-x-2599_>`_ a

  A type for lists, for example ``[1,2,3]``\.

  .. _constr-ghc-types-x-63478:

  `([]) <constr-ghc-types-x-63478_>`_


  .. _constr-ghc-types-colon-97313:

  `(:) <constr-ghc-types-colon-97313_>`_ \_ \_


Functions
---------

.. _function-da-internal-assert-assert-75697:

`assert <function-da-internal-assert-assert-75697_>`_
  \: `CanAssert <class-da-internal-assert-canassert-67323_>`_ m \=\> `Bool <type-ghc-types-bool-66265_>`_ \-\> m ()

  Check whether a condition is true\. If it's not, abort the transaction\.

.. _function-da-internal-assert-assertmsg-31545:

`assertMsg <function-da-internal-assert-assertmsg-31545_>`_
  \: `CanAssert <class-da-internal-assert-canassert-67323_>`_ m \=\> `Text <type-ghc-types-text-51952_>`_ \-\> `Bool <type-ghc-types-bool-66265_>`_ \-\> m ()

  Check whether a condition is true\. If it's not, abort the transaction
  with a message\.

.. _function-da-internal-assert-assertafter-43038:

`assertAfter <function-da-internal-assert-assertafter-43038_>`_
  \: (`CanAssert <class-da-internal-assert-canassert-67323_>`_ m, `HasTime <class-da-internal-lf-hastime-96546_>`_ m) \=\> `Time <type-da-internal-lf-time-63886_>`_ \-\> m ()

  Check whether the given time is in the future\. If it's not, abort the transaction\.

.. _function-da-internal-assert-assertbefore-99854:

`assertBefore <function-da-internal-assert-assertbefore-99854_>`_
  \: (`CanAssert <class-da-internal-assert-canassert-67323_>`_ m, `HasTime <class-da-internal-lf-hastime-96546_>`_ m) \=\> `Time <type-da-internal-lf-time-63886_>`_ \-\> m ()

  Check whether the given time is in the past\. If it's not, abort the transaction\.

.. _function-da-internal-date-dayssinceepochtodate-57782:

`daysSinceEpochToDate <function-da-internal-date-dayssinceepochtodate-57782_>`_
  \: `Int <type-ghc-types-int-37261_>`_ \-\> `Date <type-da-internal-lf-date-32253_>`_

  Convert from number of days since epoch (i\.e\. the number of days since
  January 1, 1970) to a date\.

.. _function-da-internal-date-datetodayssinceepoch-98906:

`dateToDaysSinceEpoch <function-da-internal-date-datetodayssinceepoch-98906_>`_
  \: `Date <type-da-internal-lf-date-32253_>`_ \-\> `Int <type-ghc-types-int-37261_>`_

  Convert from a date to number of days from epoch (i\.e\. the number of days
  since January 1, 1970)\.

.. _function-da-internal-interface-interfacetyperep-28987:

`interfaceTypeRep <function-da-internal-interface-interfacetyperep-28987_>`_
  \: `HasInterfaceTypeRep <class-da-internal-interface-hasinterfacetyperep-84221_>`_ i \=\> i \-\> `TemplateTypeRep <type-da-internal-any-templatetyperep-33792_>`_

  (Daml\-LF \>\= 1\.15) Obtain the ``TemplateTypeRep`` for the template given in the interface value\.

.. _function-da-internal-interface-tointerface-33774:

`toInterface <function-da-internal-interface-tointerface-33774_>`_
  \: `HasToInterface <class-da-internal-interface-hastointerface-68104_>`_ t i \=\> t \-\> i

  (Daml\-LF \>\= 1\.15) Convert a template value into an interface value\.
  For example ``toInterface @MyInterface value`` converts a template
  ``value`` into a ``MyInterface`` type\.

.. _function-da-internal-interface-tointerfacecontractid-24085:

`toInterfaceContractId <function-da-internal-interface-tointerfacecontractid-24085_>`_
  \: `HasToInterface <class-da-internal-interface-hastointerface-68104_>`_ t i \=\> `ContractId <type-da-internal-lf-contractid-95282_>`_ t \-\> `ContractId <type-da-internal-lf-contractid-95282_>`_ i

  (Daml\-LF \>\= 1\.15) Convert a template contract id into an interface
  contract id\. For example, ``toInterfaceContractId @MyInterface cid``\.

.. _function-da-internal-interface-frominterfacecontractid-39614:

`fromInterfaceContractId <function-da-internal-interface-frominterfacecontractid-39614_>`_
  \: `HasFromInterface <class-da-internal-interface-hasfrominterface-43863_>`_ t i \=\> `ContractId <type-da-internal-lf-contractid-95282_>`_ i \-\> `ContractId <type-da-internal-lf-contractid-95282_>`_ t

  (Daml\-LF \>\= 1\.15) Convert an interface contract id into a template
  contract id\. For example, ``fromInterfaceContractId @MyTemplate cid``\.

  Can also be used to convert an interface contract id into a contract id of
  one of its requiring interfaces\.

  This function does not verify that the interface contract id
  actually points to a template of the given type\. This means
  that a subsequent ``fetch``, ``exercise``, or ``archive`` may fail, if,
  for example, the contract id points to a contract that implements
  the interface but is of a different template type than expected\.

  Therefore, you should only use ``fromInterfaceContractId`` in situations
  where you already know that the contract id points to a contract of the
  right template type\. You can also use it in situations where you will
  fetch, exercise, or archive the contract right away, when a transaction
  failure is the appropriate response to the contract having the wrong
  template type\.

  In all other cases, consider using ``fetchFromInterface`` instead\.

.. _function-da-internal-interface-coerceinterfacecontractid-23361:

`coerceInterfaceContractId <function-da-internal-interface-coerceinterfacecontractid-23361_>`_
  \: (`HasInterfaceTypeRep <class-da-internal-interface-hasinterfacetyperep-84221_>`_ i, `HasInterfaceTypeRep <class-da-internal-interface-hasinterfacetyperep-84221_>`_ j) \=\> `ContractId <type-da-internal-lf-contractid-95282_>`_ i \-\> `ContractId <type-da-internal-lf-contractid-95282_>`_ j

  (Daml\-LF \>\= 1\.15) Convert an interface contract id into a contract id of a
  different interface\. For example, given two interfaces ``Source`` and ``Target``,
  and ``cid : ContractId Source``,
  ``coerceInterfaceContractId @Target @Source cid : ContractId Target``\.

  This function does not verify that the contract id
  actually points to a contract that implements either interface\. This means
  that a subsequent ``fetch``, ``exercise``, or ``archive`` may fail, if,
  for example, the contract id points to a contract of template ``A`` but it was
  coerced into a ``ContractId B`` where ``B`` is an interface and there's no
  interface instance B for A\.

  Therefore, you should only use ``coerceInterfaceContractId`` in situations
  where you already know that the contract id points to a contract of the right
  type\. You can also use it in situations where you will fetch, exercise, or
  archive the contract right away, when a transaction failure is the
  appropriate response to the contract having the wrong type\.

.. _function-da-internal-interface-fetchfrominterface-99354:

`fetchFromInterface <function-da-internal-interface-fetchfrominterface-99354_>`_
  \: (`HasFromInterface <class-da-internal-interface-hasfrominterface-43863_>`_ t i, `HasFetch <class-da-internal-template-functions-hasfetch-52387_>`_ i) \=\> `ContractId <type-da-internal-lf-contractid-95282_>`_ i \-\> `Update <type-da-internal-lf-update-68072_>`_ (`Optional <type-da-internal-prelude-optional-37153_>`_ (`ContractId <type-da-internal-lf-contractid-95282_>`_ t, t))

  (Daml\-LF \>\= 1\.15) Fetch an interface and convert it to a specific
  template type\. If conversion is succesful, this function returns
  the converted contract and its converted contract id\. Otherwise,
  this function returns ``None``\.

  Can also be used to fetch and convert an interface contract id into a
  contract and contract id of one of its requiring interfaces\.

  Example\:

  .. code-block:: daml

    do
      fetchResult <- fetchFromInterface @MyTemplate ifaceCid
      case fetchResult of
        None -> abort "Failed to convert interface to appropriate template type"
        Some (tplCid, tpl) -> do
           ... do something with tpl and tplCid ...

.. _function-da-internal-interface-exerciseinterfaceguard-49471:

`_exerciseInterfaceGuard <function-da-internal-interface-exerciseinterfaceguard-49471_>`_
  \: a \-\> b \-\> c \-\> `Bool <type-ghc-types-bool-66265_>`_

.. _function-da-internal-interface-view-38554:

`view <function-da-internal-interface-view-38554_>`_
  \: `HasInterfaceView <class-da-internal-interface-hasinterfaceview-4492_>`_ i v \=\> i \-\> v

.. _function-da-internal-lf-partytotext-91600:

`partyToText <function-da-internal-lf-partytotext-91600_>`_
  \: `Party <type-da-internal-lf-party-57932_>`_ \-\> `Text <type-ghc-types-text-51952_>`_

  Convert the ``Party`` to ``Text``, giving back what you passed to ``getParty``\.
  In most cases, you should use ``show`` instead\. ``show`` wraps
  the party in ``'ticks'`` making it clear it was a ``Party`` originally\.

.. _function-da-internal-lf-partyfromtext-17681:

`partyFromText <function-da-internal-lf-partyfromtext-17681_>`_
  \: `Text <type-ghc-types-text-51952_>`_ \-\> `Optional <type-da-internal-prelude-optional-37153_>`_ `Party <type-da-internal-lf-party-57932_>`_

  Converts a ``Text`` to ``Party``\. It returns ``None`` if the provided text contains
  any forbidden characters\. See Daml\-LF spec for a specification on which characters
  are allowed in parties\. Note that this function accepts text *without*
  single quotes\.

  This function does not check on whether the provided
  text corresponds to a party that \"exists\" on a given ledger\: it merely converts
  the given ``Text`` to a ``Party``\. The only way to guarantee that a given ``Party``
  exists on a given ledger is to involve it in a contract\.

  This function, together with ``partyToText``, forms an isomorphism between
  valid party strings and parties\. In other words, the following equations hold\:

  .. code-block:: daml
    :force:

    ∀ p. partyFromText (partyToText p) = Some p
    ∀ txt p. partyFromText txt = Some p ==> partyToText p = txt


  This function will crash at runtime if you compile Daml to Daml\-LF \< 1\.2\.

.. _function-da-internal-lf-coercecontractid-14371:

`coerceContractId <function-da-internal-lf-coercecontractid-14371_>`_
  \: `ContractId <type-da-internal-lf-contractid-95282_>`_ a \-\> `ContractId <type-da-internal-lf-contractid-95282_>`_ b

  Used to convert the type index of a ``ContractId``, since they are just
  pointers\. Note that subsequent fetches and exercises might fail if the
  template of the contract on the ledger doesn't match\.

.. _function-da-internal-prelude-curry-6393:

`curry <function-da-internal-prelude-curry-6393_>`_
  \: ((a, b) \-\> c) \-\> a \-\> b \-\> c

  Turn a function that takes a pair into a function that takes two arguments\.

.. _function-da-internal-prelude-uncurry-79420:

`uncurry <function-da-internal-prelude-uncurry-79420_>`_
  \: (a \-\> b \-\> c) \-\> (a, b) \-\> c

  Turn a function that takes two arguments into a function that takes a pair\.

.. _function-da-internal-prelude-gtgt-56231:

`(>>) <function-da-internal-prelude-gtgt-56231_>`_
  \: `Action <class-da-internal-prelude-action-68790_>`_ m \=\> m a \-\> m b \-\> m b

  Sequentially compose two actions, discarding any value produced
  by the first\. This is like sequencing operators (such as the semicolon)
  in imperative languages\.

.. _function-da-internal-prelude-ap-60204:

`ap <function-da-internal-prelude-ap-60204_>`_
  \: `Applicative <class-da-internal-prelude-applicative-9257_>`_ f \=\> f (a \-\> b) \-\> f a \-\> f b

  Synonym for ``<*>``\.

.. _function-da-internal-prelude-return-15945:

`return <function-da-internal-prelude-return-15945_>`_
  \: `Applicative <class-da-internal-prelude-applicative-9257_>`_ m \=\> a \-\> m a

  Inject a value into the monadic type\. For example, for ``Update`` and a
  value of type ``a``, ``return`` would give you an ``Update a``\.

.. _function-da-internal-prelude-join-43001:

`join <function-da-internal-prelude-join-43001_>`_
  \: `Action <class-da-internal-prelude-action-68790_>`_ m \=\> m (m a) \-\> m a

  Collapses nested actions into a single action\.

.. _function-da-internal-prelude-identity-53027:

`identity <function-da-internal-prelude-identity-53027_>`_
  \: a \-\> a

  The identity function\.

.. _function-da-internal-prelude-guard-82483:

`guard <function-da-internal-prelude-guard-82483_>`_
  \: `ActionFail <class-da-internal-prelude-actionfail-34438_>`_ m \=\> `Bool <type-ghc-types-bool-66265_>`_ \-\> m ()

.. _function-da-internal-prelude-foldl-3431:

`foldl <function-da-internal-prelude-foldl-3431_>`_
  \: (b \-\> a \-\> b) \-\> b \-\> \[a\] \-\> b

  This function is a left fold, which you can use to inspect/analyse/consume lists\.
  ``foldl f i xs`` performs a left fold over the list ``xs`` using
  the function ``f``, using the starting value ``i``\.

  Examples\:

  .. code-block:: daml

    >>> foldl (+) 0 [1,2,3]
    6

    >>> foldl (^) 10 [2,3]
    1000000


  Note that foldl works from left\-to\-right over the list arguments\.

.. _function-da-internal-prelude-find-82808:

`find <function-da-internal-prelude-find-82808_>`_
  \: (a \-\> `Bool <type-ghc-types-bool-66265_>`_) \-\> \[a\] \-\> `Optional <type-da-internal-prelude-optional-37153_>`_ a

  ``find p xs`` finds the first element of the list ``xs`` where the
  predicate ``p`` is true\. There might not be such an element, which
  is why this function returns an ``Optional a``\.

.. _function-da-internal-prelude-length-32819:

`length <function-da-internal-prelude-length-32819_>`_
  \: \[a\] \-\> `Int <type-ghc-types-int-37261_>`_

  Gives the length of the list\.

.. _function-da-internal-prelude-any-88476:

`any <function-da-internal-prelude-any-88476_>`_
  \: (a \-\> `Bool <type-ghc-types-bool-66265_>`_) \-\> \[a\] \-\> `Bool <type-ghc-types-bool-66265_>`_

  Are there any elements in the list where the predicate is true?
  ``any p xs`` is ``True`` if ``p`` holds for at least one element of ``xs``\.

.. _function-da-internal-prelude-all-93363:

`all <function-da-internal-prelude-all-93363_>`_
  \: (a \-\> `Bool <type-ghc-types-bool-66265_>`_) \-\> \[a\] \-\> `Bool <type-ghc-types-bool-66265_>`_

  Is the predicate true for all of the elements in the list?
  ``all p xs`` is ``True`` if ``p`` holds for every element of ``xs``\.

.. _function-da-internal-prelude-or-54324:

`or <function-da-internal-prelude-or-54324_>`_
  \: \[`Bool <type-ghc-types-bool-66265_>`_\] \-\> `Bool <type-ghc-types-bool-66265_>`_

  Is at least one of elements in a list of ``Bool`` true?
  ``or bs`` is ``True`` if at least one element of ``bs`` is ``True``\.

.. _function-da-internal-prelude-and-20777:

`and <function-da-internal-prelude-and-20777_>`_
  \: \[`Bool <type-ghc-types-bool-66265_>`_\] \-\> `Bool <type-ghc-types-bool-66265_>`_

  Is every element in a list of Bool true?
  ``and bs`` is ``True`` if every element of ``bs`` is ``True``\.

.. _function-da-internal-prelude-elem-84192:

`elem <function-da-internal-prelude-elem-84192_>`_
  \: `Eq <class-ghc-classes-eq-22713_>`_ a \=\> a \-\> \[a\] \-\> `Bool <type-ghc-types-bool-66265_>`_

  Does this value exist in this list?
  ``elem x xs`` is ``True`` if ``x`` is an element of the list ``xs``\.

.. _function-da-internal-prelude-notelem-95004:

`notElem <function-da-internal-prelude-notelem-95004_>`_
  \: `Eq <class-ghc-classes-eq-22713_>`_ a \=\> a \-\> \[a\] \-\> `Bool <type-ghc-types-bool-66265_>`_

  Negation of ``elem``\:
  ``elem x xs`` is ``True`` if ``x`` is *not* an element of the list ``xs``\.

.. _function-da-internal-prelude-ltdollargt-82160:

`(<$>) <function-da-internal-prelude-ltdollargt-82160_>`_
  \: `Functor <class-ghc-base-functor-31205_>`_ f \=\> (a \-\> b) \-\> f a \-\> f b

  Synonym for ``fmap``\.

.. _function-da-internal-prelude-optional-80885:

`optional <function-da-internal-prelude-optional-80885_>`_
  \: b \-\> (a \-\> b) \-\> `Optional <type-da-internal-prelude-optional-37153_>`_ a \-\> b

  The ``optional`` function takes a default value, a function, and a ``Optional``
  value\. If the ``Optional`` value is ``None``, the function returns the
  default value\. Otherwise, it applies the function to the value inside
  the ``Some`` and returns the result\.

  Basic usage examples\:

  .. code-block:: daml

    >>> optional False (> 2) (Some 3)
    True


  .. code-block:: daml

    >>> optional False (> 2) None
    False


  .. code-block:: daml

    >>> optional 0 (*2) (Some 5)
    10
    >>> optional 0 (*2) None
    0


  This example applies ``show`` to a ``Optional Int``\. If you have ``Some n``,
  this shows the underlying ``Int``, ``n``\. But if you have ``None``, this
  returns the empty string instead of (for example) ``None``\:

  .. code-block:: daml

    >>> optional "" show (Some 5)
    "5"
    >>> optional "" show (None : Optional Int)
    ""

.. _function-da-internal-prelude-either-9020:

`either <function-da-internal-prelude-either-9020_>`_
  \: (a \-\> c) \-\> (b \-\> c) \-\> `Either <type-da-types-either-56020_>`_ a b \-\> c

  The ``either`` function provides case analysis for the ``Either`` type\.
  If the value is ``Left a``, it applies the first function to ``a``;
  if it is ``Right b``, it applies the second function to ``b``\.

  Examples\:

  This example has two values of type ``Either [Int] Int``, one using the
  ``Left`` constructor and another using the ``Right`` constructor\. Then
  it applies ``either`` the ``length`` function (if it has a ``[Int]``)
  or the \"times\-two\" function (if it has an ``Int``)\:

  .. code-block:: daml

    >>> let s = Left [1,2,3] : Either [Int] Int in either length (*2) s
    3
    >>> let n = Right 3 : Either [Int] Int in either length (*2) n
    6

.. _function-da-internal-prelude-concat-86947:

`concat <function-da-internal-prelude-concat-86947_>`_
  \: \[\[a\]\] \-\> \[a\]

  Take a list of lists and concatenate those lists into one list\.

.. _function-da-internal-prelude-plusplus-64685:

`(++) <function-da-internal-prelude-plusplus-64685_>`_
  \: \[a\] \-\> \[a\] \-\> \[a\]

  Concatenate two lists\.

.. _function-da-internal-prelude-flip-60304:

`flip <function-da-internal-prelude-flip-60304_>`_
  \: (a \-\> b \-\> c) \-\> b \-\> a \-\> c

  Flip the order of the arguments of a two argument function\.

.. _function-da-internal-prelude-reverse-91564:

`reverse <function-da-internal-prelude-reverse-91564_>`_
  \: \[a\] \-\> \[a\]

  Reverse a list\.

.. _function-da-internal-prelude-mapa-5250:

`mapA <function-da-internal-prelude-mapa-5250_>`_
  \: `Applicative <class-da-internal-prelude-applicative-9257_>`_ m \=\> (a \-\> m b) \-\> \[a\] \-\> m \[b\]

  Apply an applicative function to each element of a list\.

.. _function-da-internal-prelude-fora-2999:

`forA <function-da-internal-prelude-fora-2999_>`_
  \: `Applicative <class-da-internal-prelude-applicative-9257_>`_ m \=\> \[a\] \-\> (a \-\> m b) \-\> m \[b\]

  ``forA`` is ``mapA`` with its arguments flipped\.

.. _function-da-internal-prelude-sequence-43906:

`sequence <function-da-internal-prelude-sequence-43906_>`_
  \: `Applicative <class-da-internal-prelude-applicative-9257_>`_ m \=\> \[m a\] \-\> m \[a\]

  Perform a list of actions in sequence and collect the results\.

.. _function-da-internal-prelude-eqltlt-62567:

`(=<<) <function-da-internal-prelude-eqltlt-62567_>`_
  \: `Action <class-da-internal-prelude-action-68790_>`_ m \=\> (a \-\> m b) \-\> m a \-\> m b

  ``=<<`` is ``>>=`` with its arguments flipped\.

.. _function-da-internal-prelude-concatmap-18810:

`concatMap <function-da-internal-prelude-concatmap-18810_>`_
  \: (a \-\> \[b\]) \-\> \[a\] \-\> \[b\]

  Map a function over each element of a list, and concatenate all the results\.

.. _function-da-internal-prelude-replicate-97857:

`replicate <function-da-internal-prelude-replicate-97857_>`_
  \: `Int <type-ghc-types-int-37261_>`_ \-\> a \-\> \[a\]

  ``replicate i x`` gives the list ``[x, x, x, ..., x]`` with ``i`` copies of ``x``\.

.. _function-da-internal-prelude-take-28256:

`take <function-da-internal-prelude-take-28256_>`_
  \: `Int <type-ghc-types-int-37261_>`_ \-\> \[a\] \-\> \[a\]

  Take the first ``n`` elements of a list\.

.. _function-da-internal-prelude-drop-39374:

`drop <function-da-internal-prelude-drop-39374_>`_
  \: `Int <type-ghc-types-int-37261_>`_ \-\> \[a\] \-\> \[a\]

  Drop the first ``n`` elements of a list\.

.. _function-da-internal-prelude-splitat-62285:

`splitAt <function-da-internal-prelude-splitat-62285_>`_
  \: `Int <type-ghc-types-int-37261_>`_ \-\> \[a\] \-\> (\[a\], \[a\])

  Split a list at a given index\.

.. _function-da-internal-prelude-takewhile-28496:

`takeWhile <function-da-internal-prelude-takewhile-28496_>`_
  \: (a \-\> `Bool <type-ghc-types-bool-66265_>`_) \-\> \[a\] \-\> \[a\]

  Take elements from a list while the predicate holds\.

.. _function-da-internal-prelude-dropwhile-40350:

`dropWhile <function-da-internal-prelude-dropwhile-40350_>`_
  \: (a \-\> `Bool <type-ghc-types-bool-66265_>`_) \-\> \[a\] \-\> \[a\]

  Drop elements from a list while the predicate holds\.

.. _function-da-internal-prelude-span-51013:

`span <function-da-internal-prelude-span-51013_>`_
  \: (a \-\> `Bool <type-ghc-types-bool-66265_>`_) \-\> \[a\] \-\> (\[a\], \[a\])

  ``span p xs`` is equivalent to ``(takeWhile p xs, dropWhile p xs)``\.

.. _function-da-internal-prelude-partition-74270:

`partition <function-da-internal-prelude-partition-74270_>`_
  \: (a \-\> `Bool <type-ghc-types-bool-66265_>`_) \-\> \[a\] \-\> (\[a\], \[a\])

  The ``partition`` function takes a predicate, a list and returns
  the pair of lists of elements which do and do not satisfy the
  predicate, respectively; i\.e\.,

  > partition p xs == (filter p xs, filter (not . p) xs)


  .. code-block:: daml

    >>> partition (<0) [1, -2, -3, 4, -5, 6]
    ([-2, -3, -5], [1, 4, 6])

.. _function-da-internal-prelude-break-18317:

`break <function-da-internal-prelude-break-18317_>`_
  \: (a \-\> `Bool <type-ghc-types-bool-66265_>`_) \-\> \[a\] \-\> (\[a\], \[a\])

  Break a list into two, just before the first element where the predicate holds\.
  ``break p xs`` is equivalent to ``span (not . p) xs``\.

.. _function-da-internal-prelude-lookup-7541:

`lookup <function-da-internal-prelude-lookup-7541_>`_
  \: `Eq <class-ghc-classes-eq-22713_>`_ a \=\> a \-\> \[(a, b)\] \-\> `Optional <type-da-internal-prelude-optional-37153_>`_ b

  Look up the first element with a matching key\.

.. _function-da-internal-prelude-enumerate-9854:

`enumerate <function-da-internal-prelude-enumerate-9854_>`_
  \: (`Enum <class-ghc-enum-enum-63048_>`_ a, `Bounded <class-ghc-enum-bounded-34379_>`_ a) \=\> \[a\]

  Generate a list containing all values of a given enumeration\.

.. _function-da-internal-prelude-zip-87479:

`zip <function-da-internal-prelude-zip-87479_>`_
  \: \[a\] \-\> \[b\] \-\> \[(a, b)\]

  ``zip`` takes two lists and returns a list of corresponding pairs\.
  If one list is shorter, the excess elements of the longer list are discarded\.

.. _function-da-internal-prelude-zip3-8569:

`zip3 <function-da-internal-prelude-zip3-8569_>`_
  \: \[a\] \-\> \[b\] \-\> \[c\] \-\> \[(a, b, c)\]

  ``zip3`` takes three lists and returns a list of triples, analogous to ``zip``\.

.. _function-da-internal-prelude-zipwith-13207:

`zipWith <function-da-internal-prelude-zipwith-13207_>`_
  \: (a \-\> b \-\> c) \-\> \[a\] \-\> \[b\] \-\> \[c\]

  ``zipWith`` takes a function and two lists\.
  It generalises ``zip`` by combining elements using the function, instead of forming pairs\.
  If one list is shorter, the excess elements of the longer list are discarded\.

.. _function-da-internal-prelude-zipwith3-3785:

`zipWith3 <function-da-internal-prelude-zipwith3-3785_>`_
  \: (a \-\> b \-\> c \-\> d) \-\> \[a\] \-\> \[b\] \-\> \[c\] \-\> \[d\]

  ``zipWith3`` generalises ``zip3`` by combining elements using the function, instead of forming triples\.

.. _function-da-internal-prelude-unzip-18278:

`unzip <function-da-internal-prelude-unzip-18278_>`_
  \: \[(a, b)\] \-\> (\[a\], \[b\])

  Turn a list of pairs into a pair of lists\.

.. _function-da-internal-prelude-unzip3-67086:

`unzip3 <function-da-internal-prelude-unzip3-67086_>`_
  \: \[(a, b, c)\] \-\> (\[a\], \[b\], \[c\])

  Turn a list of triples into a triple of lists\.

.. _function-da-internal-prelude-traceraw-94102:

`traceRaw <function-da-internal-prelude-traceraw-94102_>`_
  \: `Text <type-ghc-types-text-51952_>`_ \-\> a \-\> a

  ``traceRaw msg a`` prints ``msg`` and returns ``a``, for debugging purposes\.

  The default configuration on the participant logs these messages at DEBUG level\.

.. _function-da-internal-prelude-trace-56451:

`trace <function-da-internal-prelude-trace-56451_>`_
  \: `Show <class-ghc-show-show-65360_>`_ b \=\> b \-\> a \-\> a

  ``trace b a`` prints ``b`` and returns ``a``, for debugging purposes\.

  The default configuration on the participant logs these messages at DEBUG level\.

.. _function-da-internal-prelude-traceid-75962:

`traceId <function-da-internal-prelude-traceid-75962_>`_
  \: `Show <class-ghc-show-show-65360_>`_ b \=\> b \-\> b

  ``traceId a`` prints ``a`` and returns ``a``, for debugging purposes\.

  The default configuration on the participant logs these messages at DEBUG level\.

.. _function-da-internal-prelude-debug-24649:

`debug <function-da-internal-prelude-debug-24649_>`_
  \: (`Show <class-ghc-show-show-65360_>`_ b, `Action <class-da-internal-prelude-action-68790_>`_ m) \=\> b \-\> m ()

  ``debug x`` prints ``x`` for debugging purposes\.

  The default configuration on the participant logs these messages at DEBUG level\.

.. _function-da-internal-prelude-debugraw-7384:

`debugRaw <function-da-internal-prelude-debugraw-7384_>`_
  \: `Action <class-da-internal-prelude-action-68790_>`_ m \=\> `Text <type-ghc-types-text-51952_>`_ \-\> m ()

  ``debugRaw msg`` prints ``msg`` for debugging purposes\.

  The default configuration on the participant logs these messages at DEBUG level\.

.. _function-da-internal-prelude-fst-55341:

`fst <function-da-internal-prelude-fst-55341_>`_
  \: (a, b) \-\> a

  Return the first element of a tuple\.

.. _function-da-internal-prelude-snd-88623:

`snd <function-da-internal-prelude-snd-88623_>`_
  \: (a, b) \-\> b

  Return the second element of a tuple\.

.. _function-da-internal-prelude-truncate-45759:

`truncate <function-da-internal-prelude-truncate-45759_>`_
  \: `Numeric <type-ghc-types-numeric-891_>`_ n \-\> `Int <type-ghc-types-int-37261_>`_

  ``truncate x`` rounds ``x`` toward zero\.

.. _function-da-internal-prelude-inttonumeric-99696:

`intToNumeric <function-da-internal-prelude-inttonumeric-99696_>`_
  \: `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Int <type-ghc-types-int-37261_>`_ \-\> `Numeric <type-ghc-types-numeric-891_>`_ n

  Convert an ``Int`` to a ``Numeric``\.

.. _function-da-internal-prelude-inttodecimal-16628:

`intToDecimal <function-da-internal-prelude-inttodecimal-16628_>`_
  \: `Int <type-ghc-types-int-37261_>`_ \-\> `Decimal <type-ghc-types-decimal-18135_>`_

  Convert an ``Int`` to a ``Decimal``\.

.. _function-da-internal-prelude-roundbankers-80419:

`roundBankers <function-da-internal-prelude-roundbankers-80419_>`_
  \: `Int <type-ghc-types-int-37261_>`_ \-\> `Numeric <type-ghc-types-numeric-891_>`_ n \-\> `Numeric <type-ghc-types-numeric-891_>`_ n

  Bankers' Rounding\: ``roundBankers dp x`` rounds ``x`` to ``dp`` decimal places, where a ``.5`` is rounded to the nearest even digit\.

.. _function-da-internal-prelude-roundcommercial-50292:

`roundCommercial <function-da-internal-prelude-roundcommercial-50292_>`_
  \: `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Int <type-ghc-types-int-37261_>`_ \-\> `Numeric <type-ghc-types-numeric-891_>`_ n \-\> `Numeric <type-ghc-types-numeric-891_>`_ n

  Commercial Rounding\: ``roundCommercial dp x`` rounds ``x`` to ``dp`` decimal places, where a ``.5`` is rounded away from zero\.

.. _function-da-internal-prelude-round-17808:

`round <function-da-internal-prelude-round-17808_>`_
  \: `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Numeric <type-ghc-types-numeric-891_>`_ n \-\> `Int <type-ghc-types-int-37261_>`_

  Round a ``Numeric`` to the nearest integer, where a ``.5`` is rounded away from zero\.

.. _function-da-internal-prelude-floor-26810:

`floor <function-da-internal-prelude-floor-26810_>`_
  \: `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Numeric <type-ghc-types-numeric-891_>`_ n \-\> `Int <type-ghc-types-int-37261_>`_

  Round a ``Decimal`` down to the nearest integer\.

.. _function-da-internal-prelude-ceiling-19727:

`ceiling <function-da-internal-prelude-ceiling-19727_>`_
  \: `NumericScale <class-ghc-classes-numericscale-83720_>`_ n \=\> `Numeric <type-ghc-types-numeric-891_>`_ n \-\> `Int <type-ghc-types-int-37261_>`_

  Round a ``Decimal`` up to the nearest integer\.

.. _function-da-internal-prelude-null-12330:

`null <function-da-internal-prelude-null-12330_>`_
  \: \[a\] \-\> `Bool <type-ghc-types-bool-66265_>`_

  Is the list empty? ``null xs`` is true if ``xs`` is the empty list\.

.. _function-da-internal-prelude-filter-41317:

`filter <function-da-internal-prelude-filter-41317_>`_
  \: (a \-\> `Bool <type-ghc-types-bool-66265_>`_) \-\> \[a\] \-\> \[a\]

  Filters the list using the function\: keep only the elements where the predicate holds\.

.. _function-da-internal-prelude-sum-46659:

`sum <function-da-internal-prelude-sum-46659_>`_
  \: `Additive <class-ghc-num-additive-25881_>`_ a \=\> \[a\] \-\> a

  Add together all the elements in the list\.

.. _function-da-internal-prelude-product-15907:

`product <function-da-internal-prelude-product-15907_>`_
  \: `Multiplicative <class-ghc-num-multiplicative-10593_>`_ a \=\> \[a\] \-\> a

  Multiply all the elements in the list together\.

.. _function-da-internal-prelude-undefined-12708:

`undefined <function-da-internal-prelude-undefined-12708_>`_
  \: a

  A convenience function that can be used to mark something not implemented\.
  Always throws an error with \"Not implemented\.\"

.. _function-da-internal-template-functions-softfetch-60405:

`softFetch <function-da-internal-template-functions-softfetch-60405_>`_
  \: `HasSoftFetch <class-da-internal-template-functions-hassoftfetch-65731_>`_ t \=\> `ContractId <type-da-internal-lf-contractid-95282_>`_ t \-\> `Update <type-da-internal-lf-update-68072_>`_ t

.. _function-da-internal-template-functions-softexercise-80500:

`softExercise <function-da-internal-template-functions-softexercise-80500_>`_
  \: `HasSoftExercise <class-da-internal-template-functions-hassoftexercise-29758_>`_ t c r \=\> `ContractId <type-da-internal-lf-contractid-95282_>`_ t \-\> c \-\> `Update <type-da-internal-lf-update-68072_>`_ r

.. _function-da-internal-template-functions-stakeholder-47883:

`stakeholder <function-da-internal-template-functions-stakeholder-47883_>`_
  \: (`HasSignatory <class-da-internal-template-functions-hassignatory-17507_>`_ t, `HasObserver <class-da-internal-template-functions-hasobserver-3182_>`_ t) \=\> t \-\> \[`Party <type-da-internal-lf-party-57932_>`_\]

  The stakeholders of a contract\: its signatories and observers\.

.. _function-da-internal-template-functions-maintainer-44226:

`maintainer <function-da-internal-template-functions-maintainer-44226_>`_
  \: `HasMaintainer <class-da-internal-template-functions-hasmaintainer-28932_>`_ t k \=\> k \-\> \[`Party <type-da-internal-lf-party-57932_>`_\]

  The list of maintainers of a contract key\.

.. _function-da-internal-template-functions-exercisebykey-78695:

`exerciseByKey <function-da-internal-template-functions-exercisebykey-78695_>`_
  \: `HasExerciseByKey <class-da-internal-template-functions-hasexercisebykey-36549_>`_ t k c r \=\> k \-\> c \-\> `Update <type-da-internal-lf-update-68072_>`_ r

  Exercise a choice on the contract associated with the given key\.

  You must pass the ``t`` using an explicit type application\. For
  instance, if you want to exercise a choice ``Withdraw`` on a contract of
  template ``Account`` given by its key ``k``, you must call
  ``exerciseByKey @Account k Withdraw``\.

.. _function-da-internal-template-functions-createandexercise-2676:

`createAndExercise <function-da-internal-template-functions-createandexercise-2676_>`_
  \: (`HasCreate <class-da-internal-template-functions-hascreate-45738_>`_ t, `HasExercise <class-da-internal-template-functions-hasexercise-70422_>`_ t c r) \=\> t \-\> c \-\> `Update <type-da-internal-lf-update-68072_>`_ r

  Create a contract and exercise the choice on the newly created contract\.

.. _function-da-internal-template-functions-templatetyperep-92004:

`templateTypeRep <function-da-internal-template-functions-templatetyperep-92004_>`_
  \: `HasTemplateTypeRep <class-da-internal-template-functions-hastemplatetyperep-24134_>`_ t \=\> `TemplateTypeRep <type-da-internal-any-templatetyperep-33792_>`_

  Generate a unique textual representation of the template id\.

.. _function-da-internal-template-functions-toanytemplate-14372:

`toAnyTemplate <function-da-internal-template-functions-toanytemplate-14372_>`_
  \: `HasToAnyTemplate <class-da-internal-template-functions-hastoanytemplate-94418_>`_ t \=\> t \-\> `AnyTemplate <type-da-internal-any-anytemplate-63703_>`_

  Wrap the template in ``AnyTemplate``\.

  Only available for Daml\-LF 1\.7 or later\.

.. _function-da-internal-template-functions-fromanytemplate-39771:

`fromAnyTemplate <function-da-internal-template-functions-fromanytemplate-39771_>`_
  \: `HasFromAnyTemplate <class-da-internal-template-functions-hasfromanytemplate-95481_>`_ t \=\> `AnyTemplate <type-da-internal-any-anytemplate-63703_>`_ \-\> `Optional <type-da-internal-prelude-optional-37153_>`_ t

  Extract the underlying template from ``AnyTemplate`` if the type matches
  or return ``None``\.

  Only available for Daml\-LF 1\.7 or later\.

.. _function-da-internal-template-functions-toanychoice-22033:

`toAnyChoice <function-da-internal-template-functions-toanychoice-22033_>`_
  \: (`HasTemplateTypeRep <class-da-internal-template-functions-hastemplatetyperep-24134_>`_ t, `HasToAnyChoice <class-da-internal-template-functions-hastoanychoice-82571_>`_ t c r) \=\> c \-\> `AnyChoice <type-da-internal-any-anychoice-86490_>`_

  Wrap a choice in ``AnyChoice``\.

  You must pass the template type ``t`` using an explicit type application\.
  For example ``toAnyChoice @Account Withdraw``\.

  Only available for Daml\-LF 1\.7 or later\.

.. _function-da-internal-template-functions-fromanychoice-95102:

`fromAnyChoice <function-da-internal-template-functions-fromanychoice-95102_>`_
  \: (`HasTemplateTypeRep <class-da-internal-template-functions-hastemplatetyperep-24134_>`_ t, `HasFromAnyChoice <class-da-internal-template-functions-hasfromanychoice-81184_>`_ t c r) \=\> `AnyChoice <type-da-internal-any-anychoice-86490_>`_ \-\> `Optional <type-da-internal-prelude-optional-37153_>`_ c

  Extract the underlying choice from ``AnyChoice`` if the template and
  choice types match, or return ``None``\.

  You must pass the template type ``t`` using an explicit type application\.
  For example ``fromAnyChoice @Account choice``\.

  Only available for Daml\-LF 1\.7 or later\.

.. _function-da-internal-template-functions-toanycontractkey-75916:

`toAnyContractKey <function-da-internal-template-functions-toanycontractkey-75916_>`_
  \: (`HasTemplateTypeRep <class-da-internal-template-functions-hastemplatetyperep-24134_>`_ t, `HasToAnyContractKey <class-da-internal-template-functions-hastoanycontractkey-35010_>`_ t k) \=\> k \-\> `AnyContractKey <type-da-internal-any-anycontractkey-68193_>`_

  Wrap a contract key in ``AnyContractKey``\.

  You must pass the template type ``t`` using an explicit type application\.
  For example ``toAnyContractKey @Proposal k``\.

  Only available for Daml\-LF 1\.7 or later\.

.. _function-da-internal-template-functions-fromanycontractkey-60533:

`fromAnyContractKey <function-da-internal-template-functions-fromanycontractkey-60533_>`_
  \: (`HasTemplateTypeRep <class-da-internal-template-functions-hastemplatetyperep-24134_>`_ t, `HasFromAnyContractKey <class-da-internal-template-functions-hasfromanycontractkey-95587_>`_ t k) \=\> `AnyContractKey <type-da-internal-any-anycontractkey-68193_>`_ \-\> `Optional <type-da-internal-prelude-optional-37153_>`_ k

  Extract the underlying key from ``AnyContractKey`` if the template and
  choice types match, or return ``None``\.

  You must pass the template type ``t`` using an explicit type application\.
  For example ``fromAnyContractKey @Proposal k``\.

  Only available for Daml\-LF 1\.7 or later\.

.. _function-da-internal-template-functions-visiblebykey-51464:

`visibleByKey <function-da-internal-template-functions-visiblebykey-51464_>`_
  \: `HasLookupByKey <class-da-internal-template-functions-haslookupbykey-92299_>`_ t k \=\> k \-\> `Update <type-da-internal-lf-update-68072_>`_ `Bool <type-ghc-types-bool-66265_>`_

  True if contract exists, submitter is a stakeholder, and all maintainers
  authorize\. False if contract does not exist and all maintainers authorize\.
  Fails otherwise\.

.. _function-ghc-base-otherwise-74255:

`otherwise <function-ghc-base-otherwise-74255_>`_
  \: `Bool <type-ghc-types-bool-66265_>`_

  Used as an alternative in conditions\.

.. _function-ghc-base-map-90641:

`map <function-ghc-base-map-90641_>`_
  \: (a \-\> b) \-\> \[a\] \-\> \[b\]

  ``map f xs`` applies the function ``f`` to all elements of the list ``xs``
  and returns the list of results (in the same order as ``xs``)\.

.. _function-ghc-base-foldr-98040:

`foldr <function-ghc-base-foldr-98040_>`_
  \: (a \-\> b \-\> b) \-\> b \-\> \[a\] \-\> b

  This function is a right fold, which you can use to manipulate lists\.
  ``foldr f i xs`` performs a right fold over the list ``xs`` using
  the function ``f``, using the starting value ``i``\.

  Note that foldr works from right\-to\-left over the list elements\.

.. _function-ghc-base-dot-65651:

`(.) <function-ghc-base-dot-65651_>`_
  \: (b \-\> c) \-\> (a \-\> b) \-\> a \-\> c

  Composes two functions, i\.e\., ``(f . g) x = f (g x)``\.

.. _function-ghc-base-const-63840:

`const <function-ghc-base-const-63840_>`_
  \: a \-\> b \-\> a

  ``const x`` is a unary function which evaluates to ``x`` for all inputs\.

  .. code-block:: daml

    >>> const 42 "hello"
    42


  .. code-block:: daml

    >>> map (const 42) [0..3]
    [42,42,42,42]

.. _function-ghc-base-dollar-9101:

`($) <function-ghc-base-dollar-9101_>`_
  \: (a \-\> b) \-\> a \-\> b

  Take a function from ``a`` to ``b`` and a value of type ``a``, and apply the
  function to the value of type ``a``, returning a value of type ``b``\.
  This function has a very low precedence, which is why you might want to use
  it instead of regular function application\.

.. _function-ghc-classes-ampamp-27924:

`(&&) <function-ghc-classes-ampamp-27924_>`_
  \: `Bool <type-ghc-types-bool-66265_>`_ \-\> `Bool <type-ghc-types-bool-66265_>`_ \-\> `Bool <type-ghc-types-bool-66265_>`_

  Boolean \"and\"\.
  This function has short\-circuiting semantics, i\.e\., when both arguments are
  present and the first arguments evaluates to 'False', the second argument
  is not evaluated at all\.

.. _function-ghc-classes-pipepipe-13296:

`(||) <function-ghc-classes-pipepipe-13296_>`_
  \: `Bool <type-ghc-types-bool-66265_>`_ \-\> `Bool <type-ghc-types-bool-66265_>`_ \-\> `Bool <type-ghc-types-bool-66265_>`_

  Boolean \"or\"\.
  This function has short\-circuiting semantics, i\.e\., when both arguments are
  present and the first arguments evaluates to 'True', the second argument
  is not evaluated at all\.

.. _function-ghc-classes-not-45172:

`not <function-ghc-classes-not-45172_>`_
  \: `Bool <type-ghc-types-bool-66265_>`_ \-\> `Bool <type-ghc-types-bool-66265_>`_

  Boolean \"not\"

.. _function-ghc-err-error-7998:

`error <function-ghc-err-error-7998_>`_
  \: `Text <type-ghc-types-text-51952_>`_ \-\> a

  Throws a ``GeneralError`` exception\.

.. _function-ghc-num-subtract-88614:

`subtract <function-ghc-num-subtract-88614_>`_
  \: `Additive <class-ghc-num-additive-25881_>`_ a \=\> a \-\> a \-\> a

  ``subtract x y`` is equivalent to ``y - x``\.

  This is useful for partial application, e\.g\., in ``subtract 1`` since ``(- 1)`` is
  interpreted as the number ``-1`` and not a function that subtracts ``1`` from
  its argument\.

.. _function-ghc-num-x-53920:

`(%) <function-ghc-num-x-53920_>`_
  \: `Int <type-ghc-types-int-37261_>`_ \-\> `Int <type-ghc-types-int-37261_>`_ \-\> `Int <type-ghc-types-int-37261_>`_

  ``x % y`` calculates the remainder of ``x`` by ``y``

.. _function-ghc-show-shows-71951:

`shows <function-ghc-show-shows-71951_>`_
  \: `Show <class-ghc-show-show-65360_>`_ a \=\> a \-\> `ShowS <type-ghc-show-shows-46771_>`_

.. _function-ghc-show-showparen-94620:

`showParen <function-ghc-show-showparen-94620_>`_
  \: `Bool <type-ghc-types-bool-66265_>`_ \-\> `ShowS <type-ghc-show-shows-46771_>`_ \-\> `ShowS <type-ghc-show-shows-46771_>`_

  Utility function that surrounds the inner show function with
  parentheses when the 'Bool' parameter is 'True'\.

.. _function-ghc-show-showstring-40356:

`showString <function-ghc-show-showstring-40356_>`_
  \: `Text <type-ghc-types-text-51952_>`_ \-\> `ShowS <type-ghc-show-shows-46771_>`_

  Utility function converting a 'String' to a show function that
  simply prepends the string unchanged\.

.. _function-ghc-show-showspace-56114:

`showSpace <function-ghc-show-showspace-56114_>`_
  \: `ShowS <type-ghc-show-shows-46771_>`_

  Prepends a single space to the front of the string\.

.. _function-ghc-show-showcommaspace-16566:

`showCommaSpace <function-ghc-show-showcommaspace-16566_>`_
  \: `ShowS <type-ghc-show-shows-46771_>`_

  Prepends a comma and a single space to the front of the string\.
