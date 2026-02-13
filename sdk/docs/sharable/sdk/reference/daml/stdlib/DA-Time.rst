.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-time-32716:

DA.Time
=======

This module provides a set of functions to manipulate Time values\.
The ``Time`` type represents a specific datetime in UTC,
for example ``time (date 2007 Apr 5) 14 30 05``\.
The bounds for Time are 0001\-01\-01T00\:00\:00\.000000Z and
9999\-12\-31T23\:59\:59\.999999Z\.

Data Types
----------

.. _type-da-time-types-reltime-23082:

**data** `RelTime <type-da-time-types-reltime-23082_>`_

  The ``RelTime`` type describes a time offset, i\.e\. relative time\.

  **instance** Serializable `RelTime <type-da-time-types-reltime-23082_>`_

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` `RelTime <type-da-time-types-reltime-23082_>`_

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` `RelTime <type-da-time-types-reltime-23082_>`_

  **instance** :ref:`Bounded <class-ghc-enum-bounded-34379>` `RelTime <type-da-time-types-reltime-23082_>`_

  **instance** :ref:`Additive <class-ghc-num-additive-25881>` `RelTime <type-da-time-types-reltime-23082_>`_

  **instance** :ref:`Signed <class-ghc-num-signed-2671>` `RelTime <type-da-time-types-reltime-23082_>`_

  **instance** :ref:`Show <class-ghc-show-show-65360>` `RelTime <type-da-time-types-reltime-23082_>`_

Functions
---------

.. _function-da-internal-time-time-34667:

`time <function-da-internal-time-time-34667_>`_
  \: :ref:`Date <type-da-internal-lf-date-32253>` \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Time <type-da-internal-lf-time-63886>`

  ``time d h m s`` turns given UTC date ``d`` and the UTC time (given in hours, minutes, seconds)
  into a UTC timestamp (``Time``)\. Does not handle leap seconds\.

.. _function-da-time-addreltime-70617:

`addRelTime <function-da-time-addreltime-70617_>`_
  \: :ref:`Time <type-da-internal-lf-time-63886>` \-\> `RelTime <type-da-time-types-reltime-23082_>`_ \-\> :ref:`Time <type-da-internal-lf-time-63886>`

  Adjusts ``Time`` with given time offset\.

.. _function-da-time-subtime-47226:

`subTime <function-da-time-subtime-47226_>`_
  \: :ref:`Time <type-da-internal-lf-time-63886>` \-\> :ref:`Time <type-da-internal-lf-time-63886>` \-\> `RelTime <type-da-time-types-reltime-23082_>`_

  Returns time offset between two given instants\.

.. _function-da-time-wholedays-91725:

`wholeDays <function-da-time-wholedays-91725_>`_
  \: `RelTime <type-da-time-types-reltime-23082_>`_ \-\> :ref:`Int <type-ghc-types-int-37261>`

  Returns the number of whole days in a time offset\. Fraction of time is rounded towards zero\.

.. _function-da-time-days-58759:

`days <function-da-time-days-58759_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> `RelTime <type-da-time-types-reltime-23082_>`_

  A number of days in relative time\.

.. _function-da-time-hours-54068:

`hours <function-da-time-hours-54068_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> `RelTime <type-da-time-types-reltime-23082_>`_

  A number of hours in relative time\.

.. _function-da-time-minutes-72520:

`minutes <function-da-time-minutes-72520_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> `RelTime <type-da-time-types-reltime-23082_>`_

  A number of minutes in relative time\.

.. _function-da-time-seconds-68512:

`seconds <function-da-time-seconds-68512_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> `RelTime <type-da-time-types-reltime-23082_>`_

  A number of seconds in relative time\.

.. _function-da-time-milliseconds-28552:

`milliseconds <function-da-time-milliseconds-28552_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> `RelTime <type-da-time-types-reltime-23082_>`_

  A number of milliseconds in relative time\.

.. _function-da-time-microseconds-56941:

`microseconds <function-da-time-microseconds-56941_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> `RelTime <type-da-time-types-reltime-23082_>`_

  A number of microseconds in relative time\.

.. _function-da-time-convertreltimetomicroseconds-23127:

`convertRelTimeToMicroseconds <function-da-time-convertreltimetomicroseconds-23127_>`_
  \: `RelTime <type-da-time-types-reltime-23082_>`_ \-\> :ref:`Int <type-ghc-types-int-37261>`

  Convert RelTime to microseconds
  Use higher level functions instead of the internal microseconds

.. _function-da-time-convertmicrosecondstoreltime-73643:

`convertMicrosecondsToRelTime <function-da-time-convertmicrosecondstoreltime-73643_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> `RelTime <type-da-time-types-reltime-23082_>`_

  Convert microseconds to RelTime
  Use higher level functions instead of the internal microseconds

.. _function-da-time-isledgertimelt-78120:

`isLedgerTimeLT <function-da-time-isledgertimelt-78120_>`_
  \: :ref:`Time <type-da-internal-lf-time-63886>` \-\> :ref:`Update <type-da-internal-lf-update-68072>` :ref:`Bool <type-ghc-types-bool-66265>`

  True iff the ledger time of the transaction is less than the given time\.

.. _function-da-time-isledgertimele-50101:

`isLedgerTimeLE <function-da-time-isledgertimele-50101_>`_
  \: :ref:`Time <type-da-internal-lf-time-63886>` \-\> :ref:`Update <type-da-internal-lf-update-68072>` :ref:`Bool <type-ghc-types-bool-66265>`

  True iff the ledger time of the transaction is less than or equal to the given time\.

.. _function-da-time-isledgertimegt-6233:

`isLedgerTimeGT <function-da-time-isledgertimegt-6233_>`_
  \: :ref:`Time <type-da-internal-lf-time-63886>` \-\> :ref:`Update <type-da-internal-lf-update-68072>` :ref:`Bool <type-ghc-types-bool-66265>`

  True iff the ledger time of the transaction is greater than the given time\.

.. _function-da-time-isledgertimege-95212:

`isLedgerTimeGE <function-da-time-isledgertimege-95212_>`_
  \: :ref:`Time <type-da-internal-lf-time-63886>` \-\> :ref:`Update <type-da-internal-lf-update-68072>` :ref:`Bool <type-ghc-types-bool-66265>`

  True iff the ledger time of the transaction is greater than or equal to the given time\.
