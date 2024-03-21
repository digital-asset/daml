// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package interpretation

case class Limits(
    contractSignatories: Int,
    contractObservers: Int,
    choiceControllers: Int,
    choiceObservers: Int,
    choiceAuthorizers: Int,
    transactionInputContracts: Int,
)

object Limits {
  val Lenient = Limits(
    contractSignatories = Int.MaxValue,
    contractObservers = Int.MaxValue,
    choiceControllers = Int.MaxValue,
    choiceObservers = Int.MaxValue,
    choiceAuthorizers = Int.MaxValue,
    transactionInputContracts = Int.MaxValue,
  )
}
