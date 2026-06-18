.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-numeric-17471:

DA.Numeric
==========

Data Types
----------

.. _type-da-numeric-roundingmode-53864:

**data** `RoundingMode <type-da-numeric-roundingmode-53864_>`_

  Rounding modes\.

  .. _constr-da-numeric-roundingup-99831:

  `RoundingUp <constr-da-numeric-roundingup-99831_>`_

    Round away from zero\.

  .. _constr-da-numeric-roundingdown-63242:

  `RoundingDown <constr-da-numeric-roundingdown-63242_>`_

    Round towards zero\.

  .. _constr-da-numeric-roundingceiling-67218:

  `RoundingCeiling <constr-da-numeric-roundingceiling-67218_>`_

    Round towards positive infinity\.

  .. _constr-da-numeric-roundingfloor-71675:

  `RoundingFloor <constr-da-numeric-roundingfloor-71675_>`_

    Round towards negative infinity\.

  .. _constr-da-numeric-roundinghalfup-22532:

  `RoundingHalfUp <constr-da-numeric-roundinghalfup-22532_>`_

    Round towards the nearest neighbor unless both neighbors
    are equidistant, in which case round away from zero\.

  .. _constr-da-numeric-roundinghalfdown-91305:

  `RoundingHalfDown <constr-da-numeric-roundinghalfdown-91305_>`_

    Round towards the nearest neighbor unless both neighbors
    are equidistant, in which case round towards zero\.

  .. _constr-da-numeric-roundinghalfeven-70729:

  `RoundingHalfEven <constr-da-numeric-roundinghalfeven-70729_>`_

    Round towards the nearest neighbor unless both neighbors
    are equidistant, in which case round towards the even neighbor\.

  .. _constr-da-numeric-roundingunnecessary-42017:

  `RoundingUnnecessary <constr-da-numeric-roundingunnecessary-42017_>`_

    Do not round\. Raises an error if the result cannot
    be represented without rounding at the targeted scale\.

Functions
---------

.. _function-da-numeric-mul-81896:

`mul <function-da-numeric-mul-81896_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n3 \=\> :ref:`Numeric <type-ghc-types-numeric-891>` n1 \-\> :ref:`Numeric <type-ghc-types-numeric-891>` n2 \-\> :ref:`Numeric <type-ghc-types-numeric-891>` n3

  Multiply two numerics\. Both inputs and the output may have
  different scales, unlike ``(*)`` which forces all numeric scales
  to be the same\. Raises an error on overflow, rounds to chosen
  scale otherwise\.

.. _function-da-numeric-div-56407:

`div <function-da-numeric-div-56407_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n3 \=\> :ref:`Numeric <type-ghc-types-numeric-891>` n1 \-\> :ref:`Numeric <type-ghc-types-numeric-891>` n2 \-\> :ref:`Numeric <type-ghc-types-numeric-891>` n3

  Divide two numerics\. Both inputs and the output may have
  different scales, unlike ``(/)`` which forces all numeric scales
  to be the same\. Raises an error on overflow, rounds to chosen
  scale otherwise\.

.. _function-da-numeric-cast-54256:

`cast <function-da-numeric-cast-54256_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n2 \=\> :ref:`Numeric <type-ghc-types-numeric-891>` n1 \-\> :ref:`Numeric <type-ghc-types-numeric-891>` n2

  Cast a Numeric\. Raises an error on overflow or loss of precision\.

.. _function-da-numeric-castandround-59941:

`castAndRound <function-da-numeric-castandround-59941_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n2 \=\> :ref:`Numeric <type-ghc-types-numeric-891>` n1 \-\> :ref:`Numeric <type-ghc-types-numeric-891>` n2

  Cast a Numeric\. Raises an error on overflow, rounds to chosen
  scale otherwise\.

.. _function-da-numeric-shift-13796:

`shift <function-da-numeric-shift-13796_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n2 \=\> :ref:`Numeric <type-ghc-types-numeric-891>` n1 \-\> :ref:`Numeric <type-ghc-types-numeric-891>` n2

  Move the decimal point left or right by multiplying the numeric
  value by 10\^(n1 \- n2)\. Does not overflow or underflow\.

.. _function-da-numeric-pi-88702:

`pi <function-da-numeric-pi-88702_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n \=\> :ref:`Numeric <type-ghc-types-numeric-891>` n

  The number pi\.

.. _function-da-numeric-epsilon-3092:

`epsilon <function-da-numeric-epsilon-3092_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n \=\> :ref:`Numeric <type-ghc-types-numeric-891>` n

  The minimum strictly positive value that can be represented by a numeric of scale ``n``\.

.. _function-da-numeric-roundnumeric-41344:

`roundNumeric <function-da-numeric-roundnumeric-41344_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n \=\> :ref:`Int <type-ghc-types-int-37261>` \-\> `RoundingMode <type-da-numeric-roundingmode-53864_>`_ \-\> :ref:`Numeric <type-ghc-types-numeric-891>` n \-\> :ref:`Numeric <type-ghc-types-numeric-891>` n

  Round a ``Numeric`` number\. The value of ``round n r a`` is the value
  of ``a`` rounded to ``n`` decimal places (i\.e\. scale), according to the rounding
  mode ``r``\.

  This will fail when using the ``RoundingUnnecessary`` mode for a number that cannot
  be represented exactly with at most ``n`` decimal places\.
