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

  object EvaluationOrder {
    // We can't test RightToLeft evaluation order with V1 because it only works in dev and V1 tests
    // will build packages for versions 1.14 and 1.15. It works by accident in V2 at the moment
    // because there's only one V2 version: 2.dev. Eventually, right-to-left evaluation will not be
    // dev-only but instead 2.x-only, for all V2 versions. Once we've done this refactoring we can
    // remove explicit evaluation orders from the tests.
    // TODO(#17366): make RightToLeft a v2.x feature and remove evaluation order flags everywhere
    def valuesFor(majorLanguageVersion: LanguageMajorVersion): List[EvaluationOrder] =
      majorLanguageVersion match {
        case LanguageMajorVersion.V1 => List(LeftToRight)
        case LanguageMajorVersion.V2 => List(LeftToRight, RightToLeft)
      }
  }
}
