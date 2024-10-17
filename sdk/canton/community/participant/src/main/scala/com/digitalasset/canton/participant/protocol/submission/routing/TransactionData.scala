// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.MalformedInputErrors
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfLanguageVersion,
  LfVersionedTransaction,
  Stakeholders,
}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPackageId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Blinding

import scala.concurrent.{ExecutionContext, Future}

/** Bundle together some data needed to route the transaction.
  *
  * @param requiredPackagesPerParty Required packages per informee of the transaction
  * @param actAs Act as of the submitted command
  * @param readAs Read as of the submitted command
  * @param inputContractsDomainData Information about the input contracts
  * @param prescribedDomainO If non-empty, thInvalidWorkflowIde prescribed domain will be chosen for routing.
  *                          In case this domain is not admissible, submission will fail.
  */
private[routing] final case class TransactionData private (
    transaction: LfVersionedTransaction,
    ledgerTime: CantonTimestamp,
    requiredPackagesPerParty: Map[LfPartyId, Set[LfPackageId]],
    actAs: Set[LfPartyId],
    readAs: Set[LfPartyId],
    inputContractsDomainData: ContractsDomainData,
    prescribedDomainO: Option[DomainId],
) {
  val informees: Set[LfPartyId] = requiredPackagesPerParty.keySet
  val version: LfLanguageVersion = transaction.version
  val readers: Set[LfPartyId] = actAs.union(readAs)
}

private[routing] object TransactionData {
  def create(
      actAs: Set[LfPartyId],
      readAs: Set[LfPartyId],
      transaction: LfVersionedTransaction,
      ledgerTime: CantonTimestamp,
      domainStateProvider: DomainStateProvider,
      contractsStakeholders: Map[LfContractId, Stakeholders],
      disclosedContracts: Seq[LfContractId],
      prescribedDomainIdO: Option[DomainId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, TransactionRoutingError, TransactionData] =
    for {
      contractsDomainData <-
        ContractsDomainData
          .create(
            domainStateProvider,
            contractsStakeholders,
            disclosedContracts = disclosedContracts,
          )
          .leftMap[TransactionRoutingError](cids =>
            TransactionRoutingError.TopologyErrors.UnknownContractDomains
              .Error(cids.map(_.coid).toList)
          )
    } yield TransactionData(
      transaction = transaction,
      ledgerTime: CantonTimestamp,
      requiredPackagesPerParty = Blinding.partyPackages(transaction),
      actAs = actAs,
      readAs = readAs,
      inputContractsDomainData = contractsDomainData,
      prescribedDomainO = prescribedDomainIdO,
    )

  def create(
      submitterInfo: SubmitterInfo,
      transaction: LfVersionedTransaction,
      ledgerTime: CantonTimestamp,
      domainStateProvider: DomainStateProvider,
      inputContractStakeholders: Map[LfContractId, Stakeholders],
      disclosedContracts: Seq[LfContractId],
      prescribedDomainO: Option[DomainId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, TransactionRoutingError, TransactionData] = {
    def parseReader(party: Ref.Party) = LfPartyId
      .fromString(party)
      .leftMap[TransactionRoutingError](MalformedInputErrors.InvalidReader.Error.apply)

    for {
      actAs <- EitherT.fromEither[Future](submitterInfo.actAs.traverse(parseReader).map(_.toSet))
      readers <- EitherT.fromEither[Future](submitterInfo.readAs.traverse(parseReader).map(_.toSet))

      transactionData <- create(
        actAs = actAs,
        readAs = readers,
        transaction,
        ledgerTime,
        domainStateProvider,
        inputContractStakeholders,
        disclosedContracts,
        prescribedDomainO,
      )
    } yield transactionData
  }
}
