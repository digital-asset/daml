.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _reference-fixity-and-associativity:

Fixity, Associativity and Precedence
####################################

With normal, *prefix* operators (e.g. functions), the semantics of ``f g h`` is
clear: ``f`` is a function that takes ``g`` and ``h`` as parameters. If we want
``f`` to take the result of applying ``g`` to ``h`` we write ``f (g h)``.

In the case of *infix* operators (e.g. symbol operators such as ``+`` and ``*``,
or functions surrounded by backticks, for example ```elem```), it is less clear.
What does ``x - y - z`` mean? Subtracting ``x`` from ``y`` first (i.e.
``(x - y) - z``) generally yields different results than subtracting ``z`` from
``y`` first (i.e. ``x - (y - z)``). In Daml, the subraction operator ``-`` is
defined as a *left-assocative* operator. That is, when we write ``x - y - z -
...`` the parser associates *to the left*, meaning the parser interprets this as
``((x - y) - z) - ...``.

Some operators are *right-associative*. We have already encountered one:
function application! A function signature of ``a -> b -> c -> ...`` is parsed
as ``(a -> (b -> c)) -> ...``.

Finally, some operators are non-associative. A good example are comparisson
operators such as ``=`` and ``>``. This means any ambiguous usage of these
operators (e.g. ``a == b == c`` or ``a > b > c``) results in a **parse error**.

.. note::

  Non-associative operators are not to be confused with operators that are both
  left- *and* right-associative, such as ``+`` (since ``(x + y) + z = x + (y + z))``).
  To obtain a deterministic parser, these operators can be declared either as
  left or right associative. In Daml the ``+`` operator has been declared as
  left-associative

The *precedence* of operators defines, when combinding different operators,
which operator is processed first. For example, in general (and in Daml),
multiplication *takes precedence* over addition. That is, ``x + y * z`` is
parsed as ``x + (y * z)``. Operator precedence is expressed as a number, where a
higher number indicates a higher precedense. Operators of same precidence are
associated to the right (e.g. ``x + y - z`` is parsed as ``(x + y) - z``.

The fixity and precedence is declared using the ``infixl``, ``infix``, and
``infixr`` keywords (denoting left-, non-, and right-associativity,
respectfully) that take an integer between 0 and 9 inclusive and an operator the
fixity applies to. For example, ``infixl 6 +`` declares ``+`` as a
``left-associative`` operator with precedence ``6``. These keywords can be used
for user-defined operators as well. As for built-ins, the following table shows
the fixity and precedence of of Damls built-in operators:

==========  =========================================================  =======================================================================  ========================
Precedence  Left-associative                                           Non-associative                                                          Right-associative
==========  =========================================================  =======================================================================  ========================
9           ```!``                                                                                                                              ``.``
8                                                                                                                                               ``^``, ``^^``, ``**``
7           ``*``, ``/``, ```div```, ```mod```, ```rem```, ```quot```
6           ``+``, ``-``
5                                                                                                                                               ``:``, ``++``
4                                                                      ``==``, ``/=``, ``<``, ``<=``, ``>``, ``>=``, ```elem```, ```notElem```
3                                                                                                                                               ``&&``
2                                                                                                                                               ``||``
1           ``>>``, ``>>=``
0                                                                                                                                               ``$``, ``$!``, ```seq```
==========  =========================================================  =======================================================================  ========================
