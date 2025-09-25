// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive.codec

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.InteractiveSubmission.TransactionMetadataForHashing
import com.digitalasset.canton.crypto.{Hash, InteractiveSubmission}
import com.digitalasset.canton.data.LedgerTimeBoundaries
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.RoutingSynchronizerState
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo.ExternallySignedSubmission
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, TracedLogger}
import com.digitalasset.canton.platform.apiserver.execution.CommandInterpretationResult
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.EnrichedTransactionData.ExternalInputContract
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Enricher
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  GlobalKey,
  SubmittedTransaction,
  TransactionCoder,
}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.{Value, ValueCoder}
import com.google.protobuf.ByteString

import java.util.UUID
import scala.concurrent.ExecutionContext

object EnrichedTransactionData {

  /** Class that holds both the enriched FCI and the original FCI. This allows to show an enriched
    * version to the external party while maintaining the original contract instance so it stays
    * consistent with its Contract Id.
    */
  final case class ExternalInputContract(
      private val enrichedFci: FatContractInstance,
      private val originalFci: FatContractInstance,
  ) {
    require(
      enrichedFci.contractId == originalFci.contractId,
      s"Mismatching contractIds between enriched (${enrichedFci.contractId}) and original (${originalFci.contractId})",
    )

    lazy val contractId: ContractId = originalFci.contractId

    /** Return the created event blob for this contract. This does not contain any enrichment.
      */
    def toCreateEventBlob: Either[ValueCoder.EncodeError, ByteString] =
      TransactionCoder.encodeFatContractInstance(originalFci)

    /** Return the enriched contract instance. Use for encoding to the PreparedTransaction proto and
      * verifying external hash signatures.
      */
    def enrichedContract: FatContractInstance = enrichedFci

    /** Original event contract. Use within the Canton protocol.
      */
    def originalContract: FatContractInstance = originalFci
  }
}

/** Interface for an enriched transaction and input contracts.
  */
private[interactive] sealed trait EnrichedTransactionData {
  private[codec] def submitterInfo: state.SubmitterInfo
  private[codec] def transactionMeta: state.TransactionMeta
  private[codec] def transaction: SubmittedTransaction
  private[codec] def globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]]
  private[codec] def inputContracts: Map[ContractId, ExternalInputContract]
  private[codec] def synchronizerId: SynchronizerId
  private[codec] def mediatorGroup: Int
  private[codec] def transactionUUID: UUID

  def computeHash(
      hashVersion: HashingSchemeVersion,
      protocolVersion: ProtocolVersion,
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Either[InteractiveSubmission.HashError, Hash] = {
    val metadataForHashing = TransactionMetadataForHashing.create(
      submitterInfo.actAs.toSet,
      submitterInfo.commandId,
      transactionUUID,
      mediatorGroup,
      synchronizerId,
      transactionMeta.timeBoundaries,
      transactionMeta.preparationTime,
      // The hash is computed from the enriched contract because that's what the external party signs
      inputContracts.view.mapValues(_.enrichedContract).toMap,
    )
    InteractiveSubmission.computeVersionedHash(
      hashVersion,
      transaction,
      metadataForHashing,
      transactionMeta.optNodeSeeds
        .map(_.toList.toMap)
        .getOrElse(Map.empty),
      protocolVersion,
      hashTracer,
    )
  }
}

/** Transaction data for an enriched external submission during the prepare phase. This is usually
  * passed to the PreparedTransactionEncoder. DO NOT submit this transaction the protocol.
  */
final case class PrepareTransactionData(
    private[codec] val submitterInfo: state.SubmitterInfo,
    private[codec] val transactionMeta: state.TransactionMeta,
    private[codec] val transaction: SubmittedTransaction,
    private[codec] val globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
    private[codec] val inputContracts: Map[ContractId, ExternalInputContract],
    private[codec] val synchronizerId: SynchronizerId,
    private[codec] val mediatorGroup: Int,
    private[codec] val transactionUUID: UUID,
    private[codec] val maxRecordTime: Option[Timestamp],
) extends EnrichedTransactionData

/** Transaction data for an enriched external submission during the execute phase. This is usually
  * output but the PreparedTransactionDecoder. DO NOT submit this transaction to the protocol, but
  * call "impoverish" before.
  */
final case class ExecuteTransactionData(
    private[codec] val submitterInfo: state.SubmitterInfo,
    private[codec] val transactionMeta: state.TransactionMeta,
    private[codec] val transaction: SubmittedTransaction,
    private[codec] val globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
    private[codec] val inputContracts: Map[ContractId, ExternalInputContract],
    private[codec] val synchronizerId: SynchronizerId,
    private val externallySignedSubmission: ExternallySignedSubmission,
) extends EnrichedTransactionData {
  override private[codec] val mediatorGroup: Int = externallySignedSubmission.mediatorGroup.value
  override private[codec] val transactionUUID: UUID = externallySignedSubmission.transactionUUID

  def impoverish: CommandInterpretationResult = {
    val normalizedTransaction = Enricher.impoverish(transaction)
    // val normalizedInputContracts = inputContracts.view.mapValues(Enricher.impoverish).toMap
    CommandInterpretationResult(
      submitterInfo,
      transactionMeta,
      transaction = SubmittedTransaction(normalizedTransaction),
      dependsOnLedgerTime = transactionMeta.timeBoundaries != LedgerTimeBoundaries.unconstrained,
      interpretationTimeNanos = 0L, // Irrelevant here as interpretation was done during prepare,
      globalKeyMapping = globalKeyMapping,
      // Make sure to use the original contract instance here. No need to impoverish it as it hasn't been enriched
      processedDisclosedContracts = ImmArray.from(inputContracts.values.map(_.originalContract)),
      optSynchronizerId = Some(synchronizerId),
    )
  }

  def verifySignature(
      routingSynchronizerState: RoutingSynchronizerState,
      protocolVersion: ProtocolVersion,
      logger: TracedLogger,
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      externallySignedSubmission <- EitherT.fromEither[FutureUnlessShutdown](
        submitterInfo.externallySignedSubmission
          .toRight("Missing externally signed submission")
      )
      hash <- EitherT
        .fromEither[FutureUnlessShutdown](
          computeHash(
            externallySignedSubmission.version,
            protocolVersion,
          )
        )
        .leftMap(_.message)
      topologySnapshot <- EitherT
        .fromEither[FutureUnlessShutdown](
          routingSynchronizerState.getTopologySnapshotFor(synchronizerId)
        )
        .leftMap(_.cause)
      cryptoPureApi <- EitherT.fromEither[FutureUnlessShutdown](
        routingSynchronizerState
          .getSyncCryptoPureApi(synchronizerId)
          .leftMap(_.cause)
          .flatMap(_.toRight(s"Cannot verify external signature on synchronizer $synchronizerId"))
      )

      // Verify signatures
      _ <- InteractiveSubmission
        .verifySignatures(
          hash,
          externallySignedSubmission.signatures,
          cryptoPureApi,
          topologySnapshot,
          submitterInfo.actAs.toSet,
          logger,
        )
    } yield ()
}
