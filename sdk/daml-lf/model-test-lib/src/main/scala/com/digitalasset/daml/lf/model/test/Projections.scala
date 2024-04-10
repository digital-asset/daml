// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

object Projections {
  type PartySet = Set[PartyId]
  type PartyId = Int
  type ContractId = Int

  sealed trait ExerciseKind
  case object Consuming extends ExerciseKind
  case object NonConsuming extends ExerciseKind

  sealed trait Action
  final case class Create(
      contractId: ContractId,
      signatories: PartySet,
  ) extends Action
  final case class Exercise(
      kind: ExerciseKind,
      contractId: ContractId,
      controllers: PartySet,
      subTransaction: Transaction,
  ) extends Action

  type Transaction = List[Action]

  final case class Commands(actions: Transaction)

  type Projection = List[Commands]
}
