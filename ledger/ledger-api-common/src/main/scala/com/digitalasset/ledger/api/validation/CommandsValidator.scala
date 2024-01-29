// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.ledger.api.v1.commands.{Commands => ProtoCommands}

object CommandsValidator {

  /** Effective submitters of a command
    * @param actAs Guaranteed to be non-empty. Will contain exactly one element in most cases.
    * @param readAs May be empty.
    */
  case class Submitters[T](actAs: Set[T], readAs: Set[T])

  def effectiveSubmitters(commands: Option[ProtoCommands]): Submitters[String] = {
    commands.fold(noSubmitters)(effectiveSubmitters)
  }

  def effectiveSubmitters(commands: ProtoCommands): Submitters[String] = {
    val actAs = effectiveActAs(commands)
    val readAs = commands.readAs.toSet -- actAs
    Submitters(actAs, readAs)
  }

  def effectiveActAs(commands: ProtoCommands): Set[String] =
    if (commands.party.isEmpty)
      commands.actAs.toSet
    else
      commands.actAs.toSet + commands.party

  val noSubmitters: Submitters[String] = Submitters(Set.empty, Set.empty)
}
