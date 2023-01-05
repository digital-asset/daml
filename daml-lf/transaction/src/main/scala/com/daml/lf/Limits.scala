// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package interpretation

case class Limits(
    contractSignatories: Int = Int.MaxValue,
    contractObservers: Int = Int.MaxValue,
    choiceControllers: Int = Int.MaxValue,
    choiceObservers: Int = Int.MaxValue,
    transactionInputContracts: Int = Int.MaxValue,
)

object Limits {
  val Lenient = Limits()
}
