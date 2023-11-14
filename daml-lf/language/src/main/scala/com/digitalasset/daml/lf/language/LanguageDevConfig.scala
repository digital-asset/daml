// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

/** Definitions for the flags altering the behavior of the dev compiler. */
object LanguageDevConfig {

  /** The order in which applications are evaluated: from left to right or from right to left. The Daml 2.x
    * specification describes a left-to-right evaluation order. The right-to-left evaluation order is incompatible
    * with the specification: the difference is observable in the presence of exceptions or non-termination. But a
    * right-to-left evaluation order allows for a full transformation of applications to ANF form, which simplifies the
    * SExpr language and speeds up its evaluation. We intend to switch to right-to-left for Daml 3.0.
    */
  sealed abstract class EvaluationOrder extends Product with Serializable
  case object LeftToRight extends EvaluationOrder
  case object RightToLeft extends EvaluationOrder
}
