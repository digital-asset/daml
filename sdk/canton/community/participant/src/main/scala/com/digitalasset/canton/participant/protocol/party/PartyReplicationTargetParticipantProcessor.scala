// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{CryptoPureApi, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{TransactionMeta, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.event.{
  PublishesOnlinePartyReplicationEvents,
  RecordOrderPublisher,
}
import com.digitalasset.canton.protocol.{
  DriverContractMetadata,
  LfCommittedTransaction,
  LfNodeId,
  SerializableContract,
  TransactionId,
}
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, RequestCounter}
import com.digitalasset.daml.lf.CantonOnly
import com.digitalasset.daml.lf.data.{Bytes as LfBytes, ImmArray}
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** Helper trait to name parameters
  */
trait PersistsContracts {
  def persistIndexedContracts(
      acsChunkId: NonNegativeInt,
      contracts: NonEmpty[Seq[SerializableContract]],
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

/** The target participant processor ingests a party's active contracts on a specific domain and timestamp
  * from a source participant as part of Online Party Replication.
  *
  * The interaction happens via the [[com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor]]
  * API and the target participant processor enforces the protocol guarantees made by a [[PartyReplicationSourceParticipantProcessor]].
  * The following guarantees made by the target participant processor are verifiable at the party replication protocol:
  * The target participant
  * - only sends messages after receiving [[PartyReplicationSourceMessage.SourceParticipantIsReady]],
  * - requests contracts in a strictly increasing chunk id order,
  * - and sends only deserializable payloads.
  */
class PartyReplicationTargetParticipantProcessor(
    domainId: DomainId,
    participantId: ParticipantId,
    partyId: PartyId,
    partyToParticipantEffectiveAt: CantonTimestamp,
    persistContracts: PersistsContracts,
    recordOrderPublisher: PublishesOnlinePartyReplicationEvents,
    topologyClient: DomainTopologyClient,
    pureCrypto: CryptoPureApi,
    protected val protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit override val executionContext: ExecutionContext, traceContext: TraceContext)
    extends SequencerChannelProtocolProcessor {

  private val chunksRequestedExclusive = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private val chunksConsumedExclusive = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private val numberOfChunksToRequestEachTime = PositiveInt.three

  // The base hash for all indexer UpdateIds to avoid repeating this for all ACS chunks.
  private lazy val indexerUpdateIdBaseHash = pureCrypto
    .build(HashPurpose.OnlinePartyReplicationId)
    .add(partyId.toProtoPrimitive)
    .add(domainId.toProtoPrimitive)
    .add(partyToParticipantEffectiveAt.toProtoPrimitive)
    .finish()

  private lazy val partiesHostedOnTargetParticipantFUS = for {
    snapshot <- topologyClient
      .snapshotUS(
        partyToParticipantEffectiveAt.immediateSuccessor // because topology validFrom is exclusive
      )
    parties <- snapshot
      .inspectKnownParties(
        filterParty = "",
        filterParticipant = participantId.filterString,
      )
      .map(_.map(_.toLf))
  } yield parties

  override def onConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherTUtil.unitUS

  /** Consume status updates and ACS chunks from the source participant.
    */
  override def handlePayload(payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      acsChunkOrStatus <- EitherT.fromEither[FutureUnlessShutdown](
        PartyReplicationSourceMessage.fromByteString(protocolVersion)(payload).leftMap(_.message)
      )
      _ <- acsChunkOrStatus.dataOrStatus match {
        case PartyReplicationSourceMessage.SourceParticipantIsReady =>
          logger.info("Target participant notified that source participant is ready")
          requestNextSetOfChunks()
        case PartyReplicationSourceMessage.AcsChunk(chunkId, contracts) =>
          logger.debug(
            s"Received chunk $chunkId with contracts ${contracts.map(_.contract.contractId).mkString(", ")}"
          )
          // TODO(#23073): Preserve reassignment counters.
          val contractsToAdd = contracts.map(_.contract)
          for {
            _ <- persistContracts.persistIndexedContracts(chunkId, contractsToAdd)
            hostedParties <- EitherT.right(partiesHostedOnTargetParticipantFUS)
            _ <- EitherT.rightT[FutureUnlessShutdown, String](
              recordOrderPublisher.schedulePublishAddContracts(
                repairEventFromSerializedContract(chunkId, contractsToAdd, hostedParties)
              )
            )
            chunkConsumedUpToExclusive = chunksConsumedExclusive.updateAndGet(_.map(_ + 1))
            _ <-
              if (chunkConsumedUpToExclusive == chunksConsumedExclusive.get()) {
                // Create a new trace context and log the old and new trace ids, so that we don't end up with
                // a conversation that consists of only one trace id from beginning to end.
                TraceContext.withNewTraceContext { newTraceContext =>
                  logger.debug(
                    s"Target participant has received all the chunks requested before ${chunkConsumedUpToExclusive.unwrap}." +
                      s"Requesting ${numberOfChunksToRequestEachTime.unwrap} more chunks from source participant (new tid: ${newTraceContext.traceId})"
                  )
                  requestNextSetOfChunks()(newTraceContext)
                }
              } else EitherTUtil.unitUS[String]
          } yield ()
        case PartyReplicationSourceMessage.EndOfACS =>
          logger.info(
            s"Target participant has received end of data after ${chunksConsumedExclusive.get().unwrap} chunks"
          )
          recordOrderPublisher.publishBufferedEvents()
          sendCompleted("completing in response to source participant notification of end of data")
      }
    } yield ()

  private def requestNextSetOfChunks()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val updatedChunkCountRequestedExclusive =
      chunksRequestedExclusive.updateAndGet(_.map(_ + numberOfChunksToRequestEachTime.unwrap))
    val inclusiveAcsChunkCounter = updatedChunkCountRequestedExclusive.unwrap - 1
    val instruction = PartyReplicationInstruction(
      NonNegativeInt.tryCreate(inclusiveAcsChunkCounter)
    )(
      PartyReplicationInstruction.protocolVersionRepresentativeFor(protocolVersion)
    )
    sendPayload(
      s"request next set of chunks up to $inclusiveAcsChunkCounter",
      instruction.toByteString,
    )
  }

  private def repairEventFromSerializedContract(
      chunkId: NonNegativeInt,
      contracts: NonEmpty[Seq[SerializableContract]],
      hostedParties: Set[LfPartyId],
  )(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Update.RepairTransactionAccepted = {
    val hostedWitnesses =
      contracts.forgetNE.flatMap(_.metadata.stakeholders.toSeq).toSet intersect hostedParties
    val nodeIds = LazyList.from(0).map(LfNodeId)
    val txNodes = nodeIds.zip(contracts.map(_.toLf)).toMap

    def uniqueUpdateId() = {
      // Add the chunk-id to the hash to arrive at unique per-OPR updateIds.
      val hash = pureCrypto
        .build(HashPurpose.OnlinePartyReplicationId)
        .add(indexerUpdateIdBaseHash.unwrap)
        .add(chunkId.unwrap)
        .finish()
      TransactionId(hash).tryAsLedgerTransactionId
    }

    Update.RepairTransactionAccepted(
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = timestamp.toLf,
        workflowId = None,
        submissionTime = timestamp.toLf,
        submissionSeed = Update.noOpSeed,
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      transaction = LfCommittedTransaction(
        CantonOnly.lfVersionedTransaction(
          nodes = txNodes,
          roots = ImmArray.from(nodeIds.take(txNodes.size)),
        )
      ),
      updateId = uniqueUpdateId(),
      hostedWitnesses = hostedWitnesses.toList,
      contractMetadata = contracts
        .map(contract =>
          contract.contractId ->
            contract.contractSalt
              .map(DriverContractMetadata(_).toLfBytes(protocolVersion))
              .getOrElse(LfBytes.Empty)
        )
        .forgetNE
        .toMap,
      domainId = domainId,
      requestCounter = RequestCounter(1L),
      recordTime = timestamp,
    )
  }

  override def onDisconnected(status: Either[String, Unit])(implicit
      traceContext: TraceContext
  ): Unit = ()
}

object PartyReplicationTargetParticipantProcessor {
  def apply(
      domainId: DomainId,
      participantId: ParticipantId,
      partyId: PartyId,
      partyToParticipantEffectiveAt: CantonTimestamp,
      persistContracts: PersistsContracts,
      recordOrderPublisher: RecordOrderPublisher,
      topologyClient: DomainTopologyClient,
      pureCrypto: CryptoPureApi,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): PartyReplicationTargetParticipantProcessor =
    new PartyReplicationTargetParticipantProcessor(
      domainId,
      participantId,
      partyId,
      partyToParticipantEffectiveAt,
      persistContracts,
      recordOrderPublisher,
      topologyClient,
      pureCrypto,
      protocolVersion,
      timeouts,
      loggerFactory
        .append("domainId", domainId.toProtoPrimitive)
        .append("partyId", partyId.toProtoPrimitive),
    )
}
