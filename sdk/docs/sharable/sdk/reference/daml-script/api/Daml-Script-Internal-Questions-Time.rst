.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-time-87522:

Daml.Script.Internal.Questions.Time
===================================

Orphan Typeclass Instances
--------------------------

**instance** `HasTime <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-lf-hastime-96546>`_ :ref:`Script <type-daml-script-internal-lowlevel-script-4781>`

Data Types
----------

.. _type-daml-script-internal-questions-time-gettime-36498:

**data** `GetTime <type-daml-script-internal-questions-time-gettime-36498_>`_

  .. _constr-daml-script-internal-questions-time-gettime-56229:

  `GetTime <constr-daml-script-internal-questions-time-gettime-56229_>`_

    (no fields)

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `GetTime <type-daml-script-internal-questions-time-gettime-36498_>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_

.. _type-daml-script-internal-questions-time-range-12652:

**data** `Range <type-daml-script-internal-questions-time-range-12652_>`_

  Defines a range of time with an inclusive lower and upper time bound

  .. _constr-daml-script-internal-questions-time-range-98483:

  `Range <constr-daml-script-internal-questions-time-range-98483_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - min
         - `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_
         -
       * - max
         - `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_
         -

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `Range <type-daml-script-internal-questions-time-range-12652_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"max\" `Range <type-daml-script-internal-questions-time-range-12652_>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"min\" `Range <type-daml-script-internal-questions-time-range-12652_>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"timeBoundaries\" :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Range <type-daml-script-internal-questions-time-range-12652_>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"max\" `Range <type-daml-script-internal-questions-time-range-12652_>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"min\" `Range <type-daml-script-internal-questions-time-range-12652_>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"timeBoundaries\" :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>` (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Range <type-daml-script-internal-questions-time-range-12652_>`_)

.. _type-daml-script-internal-questions-time-settime-6646:

**data** `SetTime <type-daml-script-internal-questions-time-settime-6646_>`_

  .. _constr-daml-script-internal-questions-time-settime-59017:

  `SetTime <constr-daml-script-internal-questions-time-settime-59017_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - time
         - `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `SetTime <type-daml-script-internal-questions-time-settime-6646_>`_ ()

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"time\" `SetTime <type-daml-script-internal-questions-time-settime-6646_>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"time\" `SetTime <type-daml-script-internal-questions-time-settime-6646_>`_ `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_

.. _type-daml-script-internal-questions-time-sleep-74638:

**data** `Sleep <type-daml-script-internal-questions-time-sleep-74638_>`_

  .. _constr-daml-script-internal-questions-time-sleep-64533:

  `Sleep <constr-daml-script-internal-questions-time-sleep-64533_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - duration
         - `RelTime <https://docs.daml.com/daml/stdlib/DA-Time.html#type-da-time-types-reltime-23082>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `Sleep <type-daml-script-internal-questions-time-sleep-74638_>`_ ()

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"duration\" `Sleep <type-daml-script-internal-questions-time-sleep-74638_>`_ `RelTime <https://docs.daml.com/daml/stdlib/DA-Time.html#type-da-time-types-reltime-23082>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"duration\" `Sleep <type-daml-script-internal-questions-time-sleep-74638_>`_ `RelTime <https://docs.daml.com/daml/stdlib/DA-Time.html#type-da-time-types-reltime-23082>`_

Functions
---------

.. _function-daml-script-internal-questions-time-settime-32330:

`setTime <function-daml-script-internal-questions-time-settime-32330_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Time <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Set the time via the time service\.

  This is only supported in Daml Studio and ``daml test`` as well as
  when running over the gRPC API against a ledger in static time mode\.

  Note that the ledger time service does not support going backwards in time\.
  However, you can go back in time in Daml Studio\.

.. _function-daml-script-internal-questions-time-sleep-58882:

`sleep <function-daml-script-internal-questions-time-sleep-58882_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `RelTime <https://docs.daml.com/daml/stdlib/DA-Time.html#type-da-time-types-reltime-23082>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Sleep for the given duration\.

  This is primarily useful in tests
  where you repeatedly call ``query`` until a certain state is reached\.

  Note that this will sleep for the same duration in both wall clock and static time mode\.

.. _function-daml-script-internal-questions-time-passtime-50024:

`passTime <function-daml-script-internal-questions-time-passtime-50024_>`_
  \: `RelTime <https://docs.daml.com/daml/stdlib/DA-Time.html#type-da-time-types-reltime-23082>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Advance ledger time by the given interval\.

  This is only supported in Daml Studio and ``daml test`` as well as
  when running over the gRPC API against a ledger in static time mode\.
  Note that this is not an atomic operation over the
  gRPC API so no other clients should try to change time while this is
  running\.

  Note that the ledger time service does not support going backwards in time\.
  However, you can go back in time in Daml Studio\.

