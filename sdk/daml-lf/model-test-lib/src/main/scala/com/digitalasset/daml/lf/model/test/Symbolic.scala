// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.microsoft.z3._

object Symbolic {

  // ledgers

  type ParticipantIdSort = IntSort
  type ContractIdSort = IntSort
  type KeyIdSort = IntSort
  type PackageIdSort = IntSort
  type PartySort = IntSort
  type PartySetSort = SetSort[PartySort]
  type ContractIdSetSort = SetSort[ContractIdSort]

  type PartySet = ArrayExpr[PartySort, BoolSort]
  type ContractIdSet = ArrayExpr[ContractIdSort, BoolSort]
  type PartyId = IntExpr
  type ContractId = IntExpr
  type KeyId = IntExpr
  type PackageId = IntExpr

  type ParticipantId = IntExpr

  sealed trait ExerciseKind
  case object Consuming extends ExerciseKind
  case object NonConsuming extends ExerciseKind

  sealed trait Action
  final case class Create(
      contractId: ContractId,
      signatories: PartySet,
      observers: PartySet,
  ) extends Action
  final case class CreateWithKey(
      contractId: ContractId,
      keyId: KeyId,
      maintainers: PartySet,
      signatories: PartySet,
      observers: PartySet,
  ) extends Action
  final case class Exercise(
      kind: ExerciseKind,
      contractId: ContractId,
      controllers: PartySet,
      choiceObservers: PartySet,
      subTransaction: Transaction,
  ) extends Action
  final case class ExerciseByKey(
      kind: ExerciseKind,
      contractId: ContractId,
      keyId: KeyId,
      maintainers: PartySet,
      controllers: PartySet,
      choiceObservers: PartySet,
      subTransaction: Transaction,
  ) extends Action
  final case class Fetch(
      contractId: ContractId
  ) extends Action
  final case class FetchByKey(
      contractId: ContractId,
      keyId: KeyId,
      maintainers: PartySet,
  ) extends Action
  final case class LookupByKey(
      contractId: Option[ContractId],
      keyId: KeyId,
      maintainers: PartySet,
  ) extends Action
  final case class Rollback(
      subTransaction: Transaction
  ) extends Action

  type Transaction = List[Action]

  final case class Command(
      packageId: Option[PackageId],
      action: Action,
  )

  final case class Commands(
      participantId: ParticipantId,
      actAs: PartySet,
      disclosures: ContractIdSet,
      commands: List[Command],
  )

  type Ledger = List[Commands]

  // topologies

  final case class Participant(
      participantId: ParticipantId,
      parties: PartySet,
  )

  type Topology = Seq[Participant]

  // tying all together

  final case class Scenario(topology: Topology, ledger: Ledger)
}
