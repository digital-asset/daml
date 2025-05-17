// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import com.digitalasset.canton.ledger.participant.state.RoutingSynchronizerState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{LfContractId, Stakeholders}
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

private[routing] final case class ContractsSynchronizerData private (
    contractsData: Seq[ContractData]
) {
  val synchronizers: Set[PhysicalSynchronizerId] = contractsData.map(_.synchronizerId).toSet
}

private[routing] object ContractsSynchronizerData {
  /*
  For each contract, looks at which synchronizer the contract is currently assigned to.
  If the lookup succeeds for every contract, returns a right. Otherwise, returns a
  left containing all the contracts that could not be found.
   */
  def create(
      synchronizerState: RoutingSynchronizerState,
      contractsStakeholders: Map[LfContractId, Stakeholders],
      disclosedContracts: Seq[LfContractId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ContractsError, ContractsSynchronizerData] = {
    val result = synchronizerState
      .getSynchronizersOfContracts(contractsStakeholders.keySet.toSeq)
      .map { contractAssignations =>
        // Collect synchronizers of input contracts, ignoring contracts that cannot be found in the ACS.
        // Such contracts need to be ignored, because they could be divulged contracts.
        val (bad: Seq[(LfContractId, Option[PhysicalSynchronizerId])], good) =
          contractsStakeholders.toSeq
            .partitionMap { case (coid, stakeholders) =>
              contractAssignations.get(coid) match {
                case Some((synchronizerId, contractState)) if contractState.isActive =>
                  Right(ContractData(coid, synchronizerId, stakeholders))
                case Some((synchronizerId, contractState))
                    if contractState.isArchived.contains(true) =>
                  Left((coid, Some(synchronizerId)))
                case _ =>
                  Left((coid, None))
              }
            }

        // We need to diff disclosedContracts because they will not be found in the stores
        val filtered = bad
          .filterNot { case (contractId, _) => disclosedContracts.contains(contractId) }
        val (notFound, archived) = filtered.partitionMap { case (contractId, syncO) =>
          syncO.map((contractId, _)).toRight(contractId)
        }

        val contractError = ContractsError(
          notFound = notFound,
          archived = archived.groupMap { case (_, syncId) => syncId.logical } {
            case (contractId, _) => contractId
          },
        )
        if (contractError.isEmpty) Right(ContractsSynchronizerData(good)) else Left(contractError)
      }

    EitherT(result)
  }
}

private[routing] final case class ContractData(
    id: LfContractId,
    synchronizerId: PhysicalSynchronizerId,
    stakeholders: Stakeholders,
)

private[routing] final case class ContractsError(
    notFound: Seq[LfContractId],
    archived: Map[SynchronizerId, Seq[LfContractId]],
) {
  def isEmpty: Boolean = notFound.isEmpty && archived.isEmpty
}
