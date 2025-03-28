// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract, Stakeholders}

final case class ContractReassignment(
    contract: SerializableContract,
    counter: ReassignmentCounter,
) {
  def templateId = contract.rawContractInstance.contractInstance.unversioned.template
  def packageName = contract.rawContractInstance.contractInstance.unversioned.packageName
}

final case class ContractsReassignmentBatch private (
    contracts: NonEmpty[Seq[ContractReassignment]]
) {
  def contractIds: NonEmpty[Seq[LfContractId]] = contracts.map(_.contract.contractId)

  def contractIdCounters: NonEmpty[Seq[(LfContractId, ReassignmentCounter)]] = contracts.map {
    case item => (item.contract.contractId, item.counter)
  }

  def stakeholders: Stakeholders = Stakeholders(contracts.head1.contract.metadata)
}

object ContractsReassignmentBatch {
  sealed trait InvalidReassignmentBatch
  case object EmptyBatch extends InvalidReassignmentBatch
  final case class DifferingStakeholders(differing: Seq[Stakeholders])
      extends InvalidReassignmentBatch

  def apply(
      contract: SerializableContract,
      reassignmentCounter: ReassignmentCounter,
  ): ContractsReassignmentBatch = new ContractsReassignmentBatch(
    NonEmpty.mk(Seq, ContractReassignment(contract, reassignmentCounter))
  )

  def partition(
      contractCounters: Seq[(SerializableContract, ReassignmentCounter)]
  ): Seq[ContractsReassignmentBatch] =
    contractCounters
      .groupBy1 { case (contract, _) => Stakeholders(contract.metadata) }
      .map { case (_, byStakeholder) =>
        new ContractsReassignmentBatch(byStakeholder.map { case (contract, counter) =>
          ContractReassignment(contract, counter)
        })
      }
      .toSeq

  def create(
      contractCounters: Seq[(SerializableContract, ReassignmentCounter)]
  ): Either[InvalidReassignmentBatch, ContractsReassignmentBatch] =
    partition(contractCounters) match {
      case Nil => Left(EmptyBatch)
      case Seq(batch) => Right(batch)
      case more => Left(DifferingStakeholders(more.map(_.stakeholders)))
    }
}
