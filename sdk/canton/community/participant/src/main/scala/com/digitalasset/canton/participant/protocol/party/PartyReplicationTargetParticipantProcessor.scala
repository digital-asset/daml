// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.SerializableContract
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion
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
    persistContracts: PersistsContracts,
    protected val protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit override val executionContext: ExecutionContext)
    extends SequencerChannelProtocolProcessor {

  private val chunksRequestedExclusive = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private val chunksConsumedExclusive = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private val numberOfChunksToRequestEachTime = PositiveInt.three

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
          for {
            _ <- persistContracts.persistIndexedContracts(chunkId, contracts.map(_.contract))
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

  override def onDisconnected(status: Either[String, Unit])(implicit
      traceContext: TraceContext
  ): Unit = ()
}

object PartyReplicationTargetParticipantProcessor {
  def apply(
      domainId: DomainId,
      partyId: PartyId,
      persistContracts: PersistsContracts,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): PartyReplicationTargetParticipantProcessor =
    new PartyReplicationTargetParticipantProcessor(
      persistContracts,
      protocolVersion,
      timeouts,
      loggerFactory
        .append("domainId", domainId.toProtoPrimitive)
        .append("partyId", partyId.toProtoPrimitive),
    )
}
