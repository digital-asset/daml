// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.party.PartyReplicationSourceMessage.ContractWithReassignmentCounter
import com.digitalasset.canton.participant.store.AcsInspection
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** The source participant processor exposes a party's active contracts on a specified domain and timestamp
  * to a target participant as part of Online Party Replication.
  *
  * The interaction happens via the [[com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor]]
  * API and the source participant processor enforces the protocol guarantees made by a [[PartyReplicationTargetParticipantProcessor]].
  * The following guarantees made by the source participant processor are verifiable by the party replication protocol:
  * The source participant
  * - sends [[PartyReplicationSourceMessage.SourceParticipantIsReady]] when ready to send contracts,
  * - only sends as many [[PartyReplicationSourceMessage.AcsChunk]]s as requested by the target participant to honor flow control,
  * - sends [[PartyReplicationSourceMessage.AcsChunk]]s in strictly increasing and gap-free chunk id order,
  * - sends [[PartyReplicationSourceMessage.EndOfACS]] iff the processor is closed by the next message,
  * - and sends only deserializable payloads.
  *
  * @param domainId      The domain id of the domain to replicate active contracts within.
  * @param partyId       The party id of the party to replicate active contracts for.
  * @param activeAt      The timestamp on which the ACS snapshot is based, i.e. the time at which the contract to be send are active.
  * @param acsInspection Interface to inspect the ACS.
  * @param protocolVersion The protocol version to use for now for the party replication protocol. Technically the
  *                        online party replication protocol is a different protocol from the canton protocol.
  */
class PartyReplicationSourceParticipantProcessor private (
    domainId: DomainId,
    partyId: PartyId,
    activeAt: CantonTimestamp,
    acsInspection: AcsInspection, // TODO(#18525): Stream the ACS via the Ledger Api instead.
    protected val protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit override val executionContext: ExecutionContext)
    extends SequencerChannelProtocolProcessor {
  private val chunkToSendUpToExclusive = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private val numberOfContractsPerChunk = PositiveInt.two

  /** Once connected notify the target participant that the source participant is ready to be asked to send contracts.
    */
  override def onConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    // Once connected to target participant, send that source participant is ready.
    val status = PartyReplicationSourceMessage(
      PartyReplicationSourceMessage.SourceParticipantIsReady
    )(
      PartyReplicationSourceMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
    sendPayload("source participant ready", status.toByteString)
  }

  /** Handle instructions from the target participant
    */
  override def handlePayload(payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      instruction <- EitherT.fromEither[FutureUnlessShutdown](
        PartyReplicationInstruction
          .fromByteString(protocolVersion)(payload)
          .leftMap(_.message)
      )
      previousChunkToSendUpToExclusive = chunkToSendUpToExclusive.get
      _ = logger.debug(
        s"Source participant has received instruction to send up to chunk ${instruction.maxCounter}"
      )
      // Check that the target participant is requesting higher chunk numbers.
      // The party replication protocol does not support retries as a TP is expected to establish a new connection
      // after disconnects if it needs to consume previously sent chunks.
      _ <- EitherTUtil.ifThenET(
        instruction.maxCounter < previousChunkToSendUpToExclusive
      ) {
        sendError(
          s"Target participant requested non-increasing chunk ${instruction.maxCounter} compared to previous chunk $previousChunkToSendUpToExclusive"
        )
      }
      newChunkToSendUpTo = instruction.maxCounter
      contracts <- readContracts(previousChunkToSendUpToExclusive, newChunkToSendUpTo)
      _ <- sendContracts(contracts)
      sendingUpToChunk = chunkToSendUpToExclusive.updateAndGet(
        _ + NonNegativeInt.tryCreate(contracts.size)
      )

      // If there aren't enough contracts, send that we have reached the end of the ACS.
      _ <- EitherTUtil.ifThenET(sendingUpToChunk < newChunkToSendUpTo)(
        sendEndOfAcs(s"End of ACS after chunk $sendingUpToChunk")
      )
    } yield ()

  /** Reads contract chunks from the ACS in a brute-force fashion via AcsInspection until
    * TODO(#18525) reads the ACS via the Ledger API.
    */
  private def readContracts(
      newChunkToConsumerFrom: NonNegativeInt,
      newChunkToConsumeTo: NonNegativeInt,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Seq[
    (NonEmpty[Seq[ContractWithReassignmentCounter]], Int)
  ]] = {
    val contracts = List.newBuilder[ContractWithReassignmentCounter]
    performUnlessClosingEitherUSF(
      s"Read ACS from ${newChunkToConsumerFrom.unwrap} to $newChunkToConsumeTo"
    )(
      acsInspection
        .forEachVisibleActiveContract(domainId, Set(partyId.toLf), Some(activeAt)) {
          case (contract, reassignmentCounter) =>
            contracts += ContractWithReassignmentCounter(contract, reassignmentCounter)
            Right(())
        }(traceContext, executionContext)
        .bimap(
          _.toString,
          _ =>
            contracts
              .result()
              .grouped(numberOfContractsPerChunk.unwrap)
              .toSeq
              .map(NonEmpty.from(_).getOrElse(throw new IllegalStateException("Grouping failed")))
              .zipWithIndex
              .slice(newChunkToConsumerFrom.unwrap, newChunkToConsumeTo.unwrap + 1),
        )
    )
  }

  private def sendContracts(
      indexedContractChunks: Seq[(NonEmpty[Seq[ContractWithReassignmentCounter]], Int)]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.debug(
      s"Source participant sending ${indexedContractChunks.size} chunks up to ${chunkToSendUpToExclusive.get().unwrap + indexedContractChunks.size}"
    )
    MonadUtil.sequentialTraverse_(indexedContractChunks) { case (chunkContracts, index) =>
      val acsChunk = PartyReplicationSourceMessage(
        PartyReplicationSourceMessage.AcsChunk(
          NonNegativeInt.tryCreate(index),
          chunkContracts,
        )
      )(
        PartyReplicationSourceMessage.protocolVersionRepresentativeFor(protocolVersion)
      )
      sendPayload(s"ACS chunk $index", acsChunk.toByteString)
    }
  }

  private def sendEndOfAcs(endOfStreamMessage: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val endOfACS = PartyReplicationSourceMessage(
      PartyReplicationSourceMessage.EndOfACS
    )(
      PartyReplicationSourceMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
    for {
      _ <- sendPayload(endOfStreamMessage, endOfACS.toByteString)
      _ <- sendCompleted(endOfStreamMessage)
    } yield ()
  }

  override def onDisconnected(status: Either[String, Unit])(implicit
      traceContext: TraceContext
  ): Unit = ()
}

object PartyReplicationSourceParticipantProcessor {
  def apply(
      domainId: DomainId,
      partyId: PartyId,
      activeAt: CantonTimestamp,
      acsInspection: AcsInspection,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): PartyReplicationSourceParticipantProcessor =
    new PartyReplicationSourceParticipantProcessor(
      domainId,
      partyId,
      activeAt,
      acsInspection,
      protocolVersion,
      timeouts,
      loggerFactory
        .append("domainId", domainId.toProtoPrimitive)
        .append("partyId", partyId.toProtoPrimitive),
    )
}
