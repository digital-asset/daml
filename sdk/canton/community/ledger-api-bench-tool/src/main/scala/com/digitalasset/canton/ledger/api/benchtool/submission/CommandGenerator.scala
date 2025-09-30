// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.javaapi.data.Party

import scala.util.Try

trait CommandGenerator {
  def next(): Try[Seq[Command]]

  def nextUserId(): String

  def nextExtraCommandSubmitters(): List[Party]
}
