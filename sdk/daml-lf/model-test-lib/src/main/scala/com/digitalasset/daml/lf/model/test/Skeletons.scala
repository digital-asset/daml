// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.model.test

object Skeletons {

  // ledgers

  sealed trait ExerciseKind
  case object Consuming extends ExerciseKind
  case object NonConsuming extends ExerciseKind

  sealed trait Action
  final case class Create() extends Action
  final case class CreateWithKey() extends Action
  final case class Exercise(
      kind: ExerciseKind,
      subTransaction: Transaction,
  ) extends Action
  final case class ExerciseByKey(
      kind: ExerciseKind,
      subTransaction: Transaction,
  ) extends Action
  final case class Fetch() extends Action
  final case class FetchByKey() extends Action
  final case class LookupByKey(
      successful: Boolean
  ) extends Action
  final case class Rollback(subTransaction: Transaction) extends Action

  type Transaction = List[Action]

  final case class Command(
      explicitPackageId: Boolean,
      action: Action,
  )

  final case class Commands(commands: List[Command])

  type Ledger = List[Commands]

  // topologies

  final case class Participant()

  type Topology = Seq[Participant]

  // tying all together

  final case class Scenario(topology: Topology, ledger: Ledger)
}
