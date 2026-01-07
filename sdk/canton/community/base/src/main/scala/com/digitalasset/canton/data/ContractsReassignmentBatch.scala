// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId, Stakeholders}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{LfPackageId, ReassignmentCounter}
import com.digitalasset.daml.lf.data.Ref.{PackageName, TypeConId}

final case class ContractReassignment(
    contract: ContractInstance,
    sourceValidationPackageId: Source[LfPackageId],
    targetValidationPackageId: Target[LfPackageId],
    counter: ReassignmentCounter,
) {
  // TODO(#26468): Use source/target validation package ids for vetting checks in phase 3
  def templateId: TypeConId = contract.inst.templateId
  def packageName: PackageName = contract.inst.packageName
}

final case class ContractsReassignmentBatch private (
    contracts: NonEmpty[Seq[ContractReassignment]]
) {
  def contractIds: NonEmpty[Seq[LfContractId]] = contracts.map(_.contract.contractId)

  def contractIdCounters: NonEmpty[Seq[(LfContractId, ReassignmentCounter)]] = contracts.map {
    item => (item.contract.contractId, item.counter)
  }

  // TODO(#29199): Use source/target validation package ids for vetting checks
  def packageIds: Set[LfPackageId] = contracts.view.map(_.templateId.packageId).toSet

  def sourcePackageIds: Source[Set[LfPackageId]] = Source(
    contracts.view.map(_.sourceValidationPackageId.unwrap).toSet
  )
  def targetPackageIds: Target[Set[LfPackageId]] = Target(
    contracts.view.map(_.targetValidationPackageId.unwrap).toSet
  )

  def stakeholders: Stakeholders = Stakeholders(contracts.head1.contract.metadata)
}

object ContractsReassignmentBatch {
  sealed trait InvalidReassignmentBatch
  case object EmptyBatch extends InvalidReassignmentBatch
  final case class DifferingStakeholders(differing: Seq[Stakeholders])
      extends InvalidReassignmentBatch

  def apply(
      contract: ContractInstance,
      sourceValidationPackageId: Source[LfPackageId],
      targetValidationPackageId: Target[LfPackageId],
      reassignmentCounter: ReassignmentCounter,
  ): ContractsReassignmentBatch = new ContractsReassignmentBatch(
    NonEmpty.mk(
      Seq,
      ContractReassignment(
        contract,
        sourceValidationPackageId,
        targetValidationPackageId,
        reassignmentCounter,
      ),
    )
  )

  def partition(
      contractCounters: Seq[
        (ContractInstance, Source[LfPackageId], Target[LfPackageId], ReassignmentCounter)
      ]
  ): Seq[ContractsReassignmentBatch] =
    contractCounters
      .groupBy1 { case (contract, _, _, _) => Stakeholders(contract.metadata) }
      .map { case (_, byStakeholder) =>
        new ContractsReassignmentBatch(byStakeholder.map {
          case (contract, sourceValidationId, targetValidationId, counter) =>
            ContractReassignment(contract, sourceValidationId, targetValidationId, counter)
        })
      }
      .toSeq

  def create(
      contractCounters: Seq[
        (ContractInstance, Source[LfPackageId], Target[LfPackageId], ReassignmentCounter)
      ]
  ): Either[InvalidReassignmentBatch, ContractsReassignmentBatch] =
    partition(contractCounters) match {
      case Nil => Left(EmptyBatch)
      case Seq(batch) => Right(batch)
      case more => Left(DifferingStakeholders(more.map(_.stakeholders)))
    }
}
