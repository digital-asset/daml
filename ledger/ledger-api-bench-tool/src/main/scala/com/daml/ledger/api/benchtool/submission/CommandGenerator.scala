// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.client.binding.Primitive

import scala.util.Try

trait CommandGenerator {
  def next(): Try[Seq[Command]]

  def nextApplicationId(): String

  def nextExtraCommandSubmitters(): List[Primitive.Party]
}
