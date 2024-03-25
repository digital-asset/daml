// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

object Statement {

  /**  Suppresses `traverser:org.wartremover.warts.NonUnitStatements` warning on the expression level.
    *
    * @param evaluateForSideEffectOnly an expression with a side-effect that needs to be evaluated.
    * @tparam A return type of the expression that gets discarded.
    */
  @specialized def discard[A](evaluateForSideEffectOnly: A): Unit = {
    val _ = evaluateForSideEffectOnly
    () // Return unit to prevent warning due to discarding value
  }
}
