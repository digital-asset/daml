.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-tuple-81988:

DA.Tuple
========

Tuple \- Ubiquitous functions of tuples\.

Functions
---------

.. _function-da-tuple-first-48871:

`first <function-da-tuple-first-48871_>`_
  \: (a \-\> a') \-\> (a, b) \-\> (a', b)

  The pair obtained from a pair by application of a programmer
  supplied function to the argument pair's first field\.

.. _function-da-tuple-second-48360:

`second <function-da-tuple-second-48360_>`_
  \: (b \-\> b') \-\> (a, b) \-\> (a, b')

  The pair obtained from a pair by application of a programmer
  supplied function to the argument pair's second field\.

.. _function-da-tuple-both-63511:

`both <function-da-tuple-both-63511_>`_
  \: (a \-\> b) \-\> (a, a) \-\> (b, b)

  The pair obtained from a pair by application of a programmer
  supplied function to both the argument pair's first and second
  fields\.

.. _function-da-tuple-swap-76115:

`swap <function-da-tuple-swap-76115_>`_
  \: (a, b) \-\> (b, a)

  The pair obtained from a pair by permuting the order of the
  argument pair's first and second fields\.

.. _function-da-tuple-dupe-14430:

`dupe <function-da-tuple-dupe-14430_>`_
  \: a \-\> (a, a)

  Duplicate a single value into a pair\.

  > dupe 12 == (12, 12)

.. _function-da-tuple-fst3-84676:

`fst3 <function-da-tuple-fst3-84676_>`_
  \: (a, b, c) \-\> a

  Extract the 'fst' of a triple\.

.. _function-da-tuple-snd3-63950:

`snd3 <function-da-tuple-snd3-63950_>`_
  \: (a, b, c) \-\> b

  Extract the 'snd' of a triple\.

.. _function-da-tuple-thd3-58697:

`thd3 <function-da-tuple-thd3-58697_>`_
  \: (a, b, c) \-\> c

  Extract the final element of a triple\.

.. _function-da-tuple-curry3-2900:

`curry3 <function-da-tuple-curry3-2900_>`_
  \: ((a, b, c) \-\> d) \-\> a \-\> b \-\> c \-\> d

  Converts an uncurried function to a curried function\.

.. _function-da-tuple-uncurry3-51859:

`uncurry3 <function-da-tuple-uncurry3-51859_>`_
  \: (a \-\> b \-\> c \-\> d) \-\> (a, b, c) \-\> d

  Converts a curried function to a function on a triple\.
