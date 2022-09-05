.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Daml-LF experimental Builtin
============================

**This document is intended for Daml language maintainers.**

The Daml compiler and the Daml Engine provides special hooks to easily
add experimental Builtin functions to Daml-LF 1.dev. In particular,
one can add such builtins by modifying only the Daml standard library
and Speedy, but do not have to touch other parts of the stack such as
archive Protobuf definitions, compilers, type checkers, and archive
(de/en)coders.

In addition of development speed, those hooks also adds some
confidence non-development features are not impacted when prototyping
new Daml feature as few parts of the stack have to be modified.

Daml-LF Experimental Specification
----------------------------------

From Daml-LF perspective, experimental builtins are additional Daml-LF
expressions of the form::

   ExperimentalBuiltin :
     `$` t τ

Those expressions are typed as follows:

   ————————————————————————————————————————————— TyExperimentalBuiltin
     Γ  ⊢  `$` t τ : τ


Daml Standard library Hook
--------------------------

From the Compiler side, the experimental builtins are added by
convention in modules prefixed by `DA.Experimental` using primitive
type prefixed by '$'. For instance consider the following module::

  module DA.Experimental.Example where

  answer: τ
  answer = primitive @"$ANSWER"

The definition `answer` is translated in the Daml-LF definition:

  val DA.Experimental.Example:answer: τ = `$` "ANSWER" τ


Speedy Hook
-----------

From the Speedy side, the Experimental Builtins are handle by HashMap
::

  com.daml.lf.speedy.SBuiltin.SBExperimental.mapping : Map[String, SEBuilin]

that associate to translate the expression ``(`$` t τ : τ)`` to
``mapping(t)`` during Speedy compilation phase. By convention values
of `mapping` should be completely written down inside the object
`com.daml.lf.speedy.SBuiltin.SBExperimental`.


Archive decoded
---------------

Experimental builtins are a development features. They are available
in `1.dev` only.  The deserialization process will reject Any Daml-LF
stable or preview archive that contains Experimental builtins.


.. Local Variables:
.. eval: (flyspell-mode 1)
.. eval: (set-input-method "TeX")
.. End:


