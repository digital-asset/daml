// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.lf.data.Ref.Party
import com.daml.lf.engine.Blinding
import com.digitalasset.canton.ledger.participant.state.v2.SubmitterInfo
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.MalformedInputErrors
import com.digitalasset.canton.protocol.{LfContractId, LfVersionedTransaction}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPackageId, LfPartyId}

import scala.concurrent.{ExecutionContext, Future}

/** Bundle together some data needed to route the transaction.
  *
  * @param requiredPackagesPerParty Required packages per informee of the transaction
  * @param submitters Submitters of the transaction.
  * @param inputContractsDomainData Information about the input contracts
  * @param prescribedDomainO If non-empty, thInvalidWorkflowIde prescribed domain will be chosen for routing.
  *                          In case this domain is not admissible, submission will fail.
  */
private[routing] final case class TransactionData private (
    transaction: LfVersionedTransaction,
    requiredPackagesPerParty: Map[LfPartyId, Set[LfPackageId]],
    submitters: Set[LfPartyId],
    inputContractsDomainData: ContractsDomainData,
    prescribedDomainO: Option[DomainId],
) {
  val informees: Set[LfPartyId] = requiredPackagesPerParty.keySet
  val version = transaction.version
}

private[routing] object TransactionData {
  def create(
      submitters: Set[LfPartyId],
      transaction: LfVersionedTransaction,
      domainStateProvider: DomainStateProvider,
      contractRoutingParties: Map[LfContractId, Set[Party]],
      disclosedContracts: Seq[LfContractId],
      prescribedDomainId: Option[DomainId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, TransactionRoutingError, TransactionData] = {
    for {
      contractsDomainData <-
        ContractsDomainData
          .create(
            domainStateProvider,
            contractRoutingParties,
            disclosedContracts = disclosedContracts,
          )
          .leftMap[TransactionRoutingError](cids =>
            TransactionRoutingError.TopologyErrors.UnknownContractDomains
              .Error(cids.map(_.coid).toList)
          )
    } yield TransactionData(
      transaction = transaction,
      requiredPackagesPerParty = Blinding.partyPackages(transaction),
      submitters = submitters,
      inputContractsDomainData = contractsDomainData,
      prescribedDomainO = prescribedDomainId,
    )
  }

  def create(
      submitterInfo: SubmitterInfo,
      transaction: LfVersionedTransaction,
      domainStateProvider: DomainStateProvider,
      contractRoutingParties: Map[LfContractId, Set[Party]],
      disclosedContracts: Seq[LfContractId],
      submitterDomainId: Option[DomainId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, TransactionRoutingError, TransactionData] = {
    for {
      submitters <- EitherT.fromEither[Future](
        submitterInfo.actAs
          .traverse(submitter =>
            LfPartyId
              .fromString(submitter)
              .leftMap[TransactionRoutingError](MalformedInputErrors.InvalidSubmitter.Error)
          )
          .map(_.toSet)
      )

      transactionData <- create(
        submitters,
        transaction,
        domainStateProvider,
        contractRoutingParties,
        disclosedContracts,
        submitterDomainId,
      )
    } yield transactionData
  }
}
