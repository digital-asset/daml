// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.Party

import scala.concurrent.{ExecutionContext, Future}

private[routing] final case class ContractsDomainData private (
    withDomainData: Seq[ContractData]
) {
  val domains: Set[DomainId] = withDomainData.map(_.domain).toSet
}

private[routing] object ContractsDomainData {
  /*
  For each contract, looks at which domain the contract is currently assigned to.
  If the lookup succeeds for every contract, returns a right. Otherwise, returns a
  left containing all the contracts that could not be found.
   */
  def create(
      domainStateProvider: DomainStateProvider,
      contractRoutingParties: Map[LfContractId, Set[Party]],
      disclosedContracts: Seq[LfContractId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, NonEmpty[Seq[LfContractId]], ContractsDomainData] = {
    val result = domainStateProvider
      .getDomainsOfContracts(contractRoutingParties.keySet.toSeq)
      .map { domainMap =>
        // Collect domains of input contracts, ignoring contracts that cannot be found in the ACS.
        // Such contracts need to be ignored, because they could be divulged contracts.
        val (bad, good) = contractRoutingParties.toSeq
          .partitionMap { case (coid, routingParties) =>
            domainMap.get(coid) match {
              case Some(domainId) =>
                Right(ContractData(coid, domainId, routingParties))
              case None => Left(coid)
            }
          }

        // We need to diff disclosedContracts because they will not be found in the stores
        NonEmpty.from(bad.diff(disclosedContracts)).toLeft(good).map(ContractsDomainData(_))
      }

    EitherT(result)
  }
}

private[routing] final case class ContractData(
    id: LfContractId,
    domain: DomainId,
    stakeholders: Set[LfPartyId],
)
