// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.client.binding.Primitive

import scala.util.Try

/** Allows for splitting generated commands in two separate transactions.
  */
case class GeneratedCommands(
    firstTransaction: Seq[Command],
    secondTransaction: Seq[Command] = Seq.empty,
) {

  def isEmpty: Boolean = firstTransaction.isEmpty && secondTransaction.isEmpty

}
trait CommandGenerator {
  def next(): Try[GeneratedCommands]

  def nextApplicationId(): String

  def nextExtraCommandSubmitters(): List[Primitive.Party]
}
