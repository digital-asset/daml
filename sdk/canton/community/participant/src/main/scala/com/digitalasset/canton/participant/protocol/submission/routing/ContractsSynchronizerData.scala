// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{LfContractId, Stakeholders}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

private[routing] final case class ContractsSynchronizerData private (
    contractsData: Seq[ContractData]
) {
  val synchronizers: Set[SynchronizerId] = contractsData.map(_.synchronizerId).toSet
}

private[routing] object ContractsSynchronizerData {
  /*
  For each contract, looks at which domain the contract is currently assigned to.
  If the lookup succeeds for every contract, returns a right. Otherwise, returns a
  left containing all the contracts that could not be found.
   */
  def create(
      synchronizerStateProvider: SynchronizerStateProvider,
      contractsStakeholders: Map[LfContractId, Stakeholders],
      disclosedContracts: Seq[LfContractId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, NonEmpty[Seq[LfContractId]], ContractsSynchronizerData] = {
    val result = synchronizerStateProvider
      .getSynchronizersOfContracts(contractsStakeholders.keySet.toSeq)
      .map { contractAssignations =>
        // Collect synchronizers of input contracts, ignoring contracts that cannot be found in the ACS.
        // Such contracts need to be ignored, because they could be divulged contracts.
        val (bad, good) = contractsStakeholders.toSeq
          .partitionMap { case (coid, stakeholders) =>
            contractAssignations.get(coid) match {
              case Some(synchronizerId) =>
                Right(ContractData(coid, synchronizerId, stakeholders))
              case None => Left(coid)
            }
          }

        // We need to diff disclosedContracts because they will not be found in the stores
        NonEmpty.from(bad.diff(disclosedContracts)).toLeft(good).map(ContractsSynchronizerData(_))
      }

    EitherT(result)
  }
}

private[routing] final case class ContractData(
    id: LfContractId,
    synchronizerId: SynchronizerId,
    stakeholders: Stakeholders,
)
