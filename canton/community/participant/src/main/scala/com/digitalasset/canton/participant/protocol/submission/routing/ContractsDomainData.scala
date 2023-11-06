// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import com.daml.lf.data.Ref.Party
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

private[routing] final case class ContractsDomainData(
    withDomainData: Seq[ContractData],
    withoutDomainData: Seq[LfContractId],
) {
  val domains: Set[DomainId] = withDomainData.map(_.domain).toSet
}

private[routing] object ContractsDomainData {
  def create(
      domainStateProvider: DomainStateProvider,
      contractRoutingParties: Map[LfContractId, Set[Party]],
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[ContractsDomainData] = {
    domainStateProvider
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

        ContractsDomainData(good, bad)
      }
  }
}

private[routing] final case class ContractData(
    id: LfContractId,
    domain: DomainId,
    stakeholders: Set[LfPartyId],
)
