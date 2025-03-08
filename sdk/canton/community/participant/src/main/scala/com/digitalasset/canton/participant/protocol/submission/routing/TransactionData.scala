// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo.ExternallySignedSubmission
import com.digitalasset.canton.ledger.participant.state.{RoutingSynchronizerState, SubmitterInfo}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfLanguageVersion,
  LfVersionedTransaction,
  Stakeholders,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPackageId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.IdString
import com.digitalasset.daml.lf.engine.Blinding

import scala.concurrent.ExecutionContext

import TransactionRoutingError.MalformedInputErrors

/** Bundle together some data needed to route the transaction.
  *
  * @param requiredPackagesPerParty
  *   Required packages per informee of the transaction
  * @param actAs
  *   Act as of the submitted command
  * @param readAs
  *   Read as of the submitted command
  * @param inputContractsSynchronizerData
  *   Information about the input contracts
  * @param prescribedSynchronizerIdO
  *   If non-empty, thInvalidWorkflowIde prescribed synchronizer will be chosen for routing. In case
  *   this synchronizer is not admissible, submission will fail.
  * @param externallySignedSubmissionO
  *   Data for externally signed transactions. Can be empty.
  */
private[routing] final case class TransactionData private (
    transaction: LfVersionedTransaction,
    ledgerTime: CantonTimestamp,
    requiredPackagesPerParty: Map[LfPartyId, Set[LfPackageId]],
    actAs: Set[LfPartyId],
    readAs: Set[LfPartyId],
    externallySignedSubmissionO: Option[ExternallySignedSubmission],
    inputContractsSynchronizerData: ContractsSynchronizerData,
    prescribedSynchronizerIdO: Option[SynchronizerId],
) {
  val informees: Set[LfPartyId] = requiredPackagesPerParty.keySet
  val version: LfLanguageVersion = transaction.version
  val readers: Set[LfPartyId] = actAs.union(readAs)
}

private[routing] object TransactionData {
  def create(
      actAs: Set[LfPartyId],
      readAs: Set[LfPartyId],
      externallySignedSubmissionO: Option[ExternallySignedSubmission],
      transaction: LfVersionedTransaction,
      ledgerTime: CantonTimestamp,
      synchronizerState: RoutingSynchronizerState,
      contractsStakeholders: Map[LfContractId, Stakeholders],
      disclosedContracts: Seq[LfContractId],
      prescribedSynchronizerIdO: Option[SynchronizerId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, TransactionData] =
    for {
      contractsSynchronizerData <-
        ContractsSynchronizerData
          .create(
            synchronizerState,
            contractsStakeholders,
            disclosedContracts = disclosedContracts,
          )
          .leftMap[TransactionRoutingError](cids =>
            TransactionRoutingError.TopologyErrors.UnknownContractSynchronizers
              .Error(cids.map(_.coid).toList)
          )
    } yield TransactionData(
      transaction = transaction,
      ledgerTime: CantonTimestamp,
      requiredPackagesPerParty = Blinding.partyPackages(transaction),
      actAs = actAs,
      readAs = readAs,
      externallySignedSubmissionO = externallySignedSubmissionO,
      inputContractsSynchronizerData = contractsSynchronizerData,
      prescribedSynchronizerIdO = prescribedSynchronizerIdO,
    )

  def create(
      submitterInfo: SubmitterInfo,
      transaction: LfVersionedTransaction,
      ledgerTime: CantonTimestamp,
      synchronizerState: RoutingSynchronizerState,
      inputContractStakeholders: Map[LfContractId, Stakeholders],
      disclosedContracts: Seq[LfContractId],
      prescribedSynchronizerO: Option[SynchronizerId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, TransactionData] = {
    def parseReader(party: Ref.Party): Either[TransactionRoutingError, IdString.Party] = LfPartyId
      .fromString(party)
      .leftMap[TransactionRoutingError](MalformedInputErrors.InvalidReader.Error.apply)

    for {
      actAs <- EitherT.fromEither[FutureUnlessShutdown](
        submitterInfo.actAs.traverse(parseReader).map(_.toSet)
      )
      readers <- EitherT.fromEither[FutureUnlessShutdown](
        submitterInfo.readAs.traverse(parseReader).map(_.toSet)
      )

      transactionData <- create(
        actAs = actAs,
        readAs = readers,
        externallySignedSubmissionO = submitterInfo.externallySignedSubmission,
        transaction,
        ledgerTime,
        synchronizerState,
        inputContractStakeholders,
        disclosedContracts,
        prescribedSynchronizerO,
      )
    } yield transactionData
  }
}
