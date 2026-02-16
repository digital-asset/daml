.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-date-80009:

DA.Date
=======

This module provides a set of functions to manipulate Date values\.
The bounds for Date are 0001\-01\-01T00\:00\:00\.000000Z and
9999\-12\-31T23\:59\:59\.999999Z\.

Data Types
----------

.. _type-da-date-types-dayofweek-18120:

**data** `DayOfWeek <type-da-date-types-dayofweek-18120_>`_

  .. _constr-da-date-types-monday-43349:

  `Monday <constr-da-date-types-monday-43349_>`_


  .. _constr-da-date-types-tuesday-5501:

  `Tuesday <constr-da-date-types-tuesday-5501_>`_


  .. _constr-da-date-types-wednesday-18786:

  `Wednesday <constr-da-date-types-wednesday-18786_>`_


  .. _constr-da-date-types-thursday-55301:

  `Thursday <constr-da-date-types-thursday-55301_>`_


  .. _constr-da-date-types-friday-14884:

  `Friday <constr-da-date-types-friday-14884_>`_


  .. _constr-da-date-types-saturday-99714:

  `Saturday <constr-da-date-types-saturday-99714_>`_


  .. _constr-da-date-types-sunday-48181:

  `Sunday <constr-da-date-types-sunday-48181_>`_


  **instance** Serializable `DayOfWeek <type-da-date-types-dayofweek-18120_>`_

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` `DayOfWeek <type-da-date-types-dayofweek-18120_>`_

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` `DayOfWeek <type-da-date-types-dayofweek-18120_>`_

  **instance** :ref:`Bounded <class-ghc-enum-bounded-34379>` `DayOfWeek <type-da-date-types-dayofweek-18120_>`_

  **instance** :ref:`Enum <class-ghc-enum-enum-63048>` `DayOfWeek <type-da-date-types-dayofweek-18120_>`_

  **instance** :ref:`Show <class-ghc-show-show-65360>` `DayOfWeek <type-da-date-types-dayofweek-18120_>`_

.. _type-da-date-types-month-22803:

**data** `Month <type-da-date-types-month-22803_>`_

  The ``Month`` type represents a month in the Gregorian calendar\.

  Note that, while ``Month`` has an ``Enum`` instance, the ``toEnum`` and ``fromEnum``
  functions start counting at 0, i\.e\. ``toEnum 1 :: Month`` is ``Feb``\.

  .. _constr-da-date-types-jan-1103:

  `Jan <constr-da-date-types-jan-1103_>`_


  .. _constr-da-date-types-feb-88523:

  `Feb <constr-da-date-types-feb-88523_>`_


  .. _constr-da-date-types-mar-5472:

  `Mar <constr-da-date-types-mar-5472_>`_


  .. _constr-da-date-types-apr-12091:

  `Apr <constr-da-date-types-apr-12091_>`_


  .. _constr-da-date-types-may-50999:

  `May <constr-da-date-types-may-50999_>`_


  .. _constr-da-date-types-jun-17739:

  `Jun <constr-da-date-types-jun-17739_>`_


  .. _constr-da-date-types-jul-21893:

  `Jul <constr-da-date-types-jul-21893_>`_


  .. _constr-da-date-types-aug-18125:

  `Aug <constr-da-date-types-aug-18125_>`_


  .. _constr-da-date-types-sep-63548:

  `Sep <constr-da-date-types-sep-63548_>`_


  .. _constr-da-date-types-oct-96134:

  `Oct <constr-da-date-types-oct-96134_>`_


  .. _constr-da-date-types-nov-72317:

  `Nov <constr-da-date-types-nov-72317_>`_


  .. _constr-da-date-types-dec-74760:

  `Dec <constr-da-date-types-dec-74760_>`_


  **instance** Serializable `Month <type-da-date-types-month-22803_>`_

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` `Month <type-da-date-types-month-22803_>`_

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` `Month <type-da-date-types-month-22803_>`_

  **instance** :ref:`Bounded <class-ghc-enum-bounded-34379>` `Month <type-da-date-types-month-22803_>`_

  **instance** :ref:`Enum <class-ghc-enum-enum-63048>` `Month <type-da-date-types-month-22803_>`_

  **instance** :ref:`Show <class-ghc-show-show-65360>` `Month <type-da-date-types-month-22803_>`_

Functions
---------

.. _function-da-date-adddays-7836:

`addDays <function-da-date-adddays-7836_>`_
  \: :ref:`Date <type-da-internal-lf-date-32253>` \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Date <type-da-internal-lf-date-32253>`

  Add the given number of days to a date\.

.. _function-da-date-subtractdays-16626:

`subtractDays <function-da-date-subtractdays-16626_>`_
  \: :ref:`Date <type-da-internal-lf-date-32253>` \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Date <type-da-internal-lf-date-32253>`

  Subtract the given number of days from a date\.

  ``subtractDays d r`` is equivalent to ``addDays d (- r)``\.

.. _function-da-date-subdate-25598:

`subDate <function-da-date-subdate-25598_>`_
  \: :ref:`Date <type-da-internal-lf-date-32253>` \-\> :ref:`Date <type-da-internal-lf-date-32253>` \-\> :ref:`Int <type-ghc-types-int-37261>`

  Returns the number of days between the two given dates\.

.. _function-da-date-dayofweek-99931:

`dayOfWeek <function-da-date-dayofweek-99931_>`_
  \: :ref:`Date <type-da-internal-lf-date-32253>` \-\> `DayOfWeek <type-da-date-types-dayofweek-18120_>`_

  Returns the day of week for the given date\.

.. _function-da-date-fromgregorian-85346:

`fromGregorian <function-da-date-fromgregorian-85346_>`_
  \: (:ref:`Int <type-ghc-types-int-37261>`, `Month <type-da-date-types-month-22803_>`_, :ref:`Int <type-ghc-types-int-37261>`) \-\> :ref:`Date <type-da-internal-lf-date-32253>`

  Constructs a ``Date`` from the triplet ``(year, month, days)``\.

.. _function-da-date-togregorian-84541:

`toGregorian <function-da-date-togregorian-84541_>`_
  \: :ref:`Date <type-da-internal-lf-date-32253>` \-\> (:ref:`Int <type-ghc-types-int-37261>`, `Month <type-da-date-types-month-22803_>`_, :ref:`Int <type-ghc-types-int-37261>`)

  Turn ``Date`` value into a ``(year, month, day)`` triple, according
  to the Gregorian calendar\.

.. _function-da-date-date-21355:

`date <function-da-date-date-21355_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> `Month <type-da-date-types-month-22803_>`_ \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Date <type-da-internal-lf-date-32253>`

  Given the three values (year, month, day), constructs a ``Date`` value\.
  ``date y m d`` turns the year ``y``, month ``m``, and day ``d`` into a ``Date`` value\.
  Raises an error if ``d`` is outside the range ``1 .. monthDayCount y m``\.

.. _function-da-date-isleapyear-61920:

`isLeapYear <function-da-date-isleapyear-61920_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Returns ``True`` if the given year is a leap year\.

.. _function-da-date-frommonth-90328:

`fromMonth <function-da-date-frommonth-90328_>`_
  \: `Month <type-da-date-types-month-22803_>`_ \-\> :ref:`Int <type-ghc-types-int-37261>`

  Get the number corresponding to given month\. For example, ``Jan`` corresponds
  to ``1``, ``Feb`` corresponds to ``2``, and so on\.

.. _function-da-date-monthdaycount-59295:

`monthDayCount <function-da-date-monthdaycount-59295_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> `Month <type-da-date-types-month-22803_>`_ \-\> :ref:`Int <type-ghc-types-int-37261>`

  Get number of days in the given month in the given year, according to Gregorian calendar\.
  This does not take historical calendar changes into account (for example, the
  moves from Julian to Gregorian calendar), but does count leap years\.

.. _function-da-date-datetime-90284:

`datetime <function-da-date-datetime-90284_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> `Month <type-da-date-types-month-22803_>`_ \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Time <type-da-internal-lf-time-63886>`

  Constructs an instant using ``year``, ``month``, ``day``, ``hours``, ``minutes``, ``seconds``\.

.. _function-da-date-todateutc-87953:

`toDateUTC <function-da-date-todateutc-87953_>`_
  \: :ref:`Time <type-da-internal-lf-time-63886>` \-\> :ref:`Date <type-da-internal-lf-date-32253>`

  Extracts UTC date from UTC time\.

  This function will truncate Time to Date, but in many cases it will not return the date you really want\.
  The reason for this is that usually the source of Time would be getTime, and getTime returns UTC, and most likely
  the date you want is something local to a location or an exchange\. Consequently the date retrieved this way would be
  yesterday if retrieved when the market opens in say Singapore\.
