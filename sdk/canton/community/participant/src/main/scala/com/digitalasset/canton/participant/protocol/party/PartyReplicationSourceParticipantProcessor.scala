// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.data.ActiveContractOld
import com.digitalasset.canton.participant.admin.party.PartyReplicationTestInterceptor
import com.digitalasset.canton.participant.store.AcsInspection
import com.digitalasset.canton.participant.store.AcsInspectionError.SerializationIssue
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.SerializableContract
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** The source participant processor exposes a party's active contracts on a specified synchronizer
  * and timestamp to a target participant as part of Online Party Replication.
  *
  * The interaction happens via the
  * [[com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor]] API and
  * the source participant processor enforces the protocol guarantees made by a
  * [[PartyReplicationTargetParticipantProcessor]]. The following guarantees made by the source
  * participant processor are verifiable by the party replication protocol: The source participant
  *   - only sends messages after receiving [[PartyReplicationTargetParticipantMessage.Initialize]],
  *   - only sends as many contracts as requested by the target participant to honor flow control,
  *   - sends [[PartyReplicationSourceParticipantMessage.EndOfACS]] as the last message,
  *   - and sends only deserializable payloads.
  *
  * @param psid
  *   The synchronizer id of the synchronizer to replicate active contracts within.
  * @param partyId
  *   The party id of the party to replicate active contracts for.
  * @param activeAfter
  *   The timestamp immediately after which the ACS snapshot is based, i.e. the time immediately
  *   after which the contract to be sent are active.
  * @param otherPartiesHostedByTargetParticipant
  *   The set of parties already hosted by the target participant (TP) other than the party being
  *   replicated. Used to skip over shared contracts already hosted on TP.
  * @param acsInspection
  *   Interface to inspect the ACS.
  * @param onAcsFullyReplicated
  *   Callback notification that the source participant has sent the entire ACS.
  * @param onError
  *   Callback notification that the source participant has encountered an error.
  * @param onDisconnect
  *   Callback notification that the target participant has disconnected.
  * @param testOnlyInterceptor
  *   Test interceptor only alters behavior in integration tests.
  */
final class PartyReplicationSourceParticipantProcessor private (
    val psid: PhysicalSynchronizerId,
    partyId: PartyId,
    activeAfter: CantonTimestamp,
    // TODO(#23097): Revisit mechanism to consider "other parties" once we support support multiple concurrent OnPRs
    //  as the set of other parties would change dynamically.
    otherPartiesHostedByTargetParticipant: Set[LfPartyId],
    acsInspection: AcsInspection, // TODO(#24326): Stream the ACS via the Ledger Api instead.
    protected val onAcsFullyReplicated: TraceContext => Unit,
    protected val onError: String => Unit,
    protected val onDisconnect: (String, TraceContext) => Unit,
    protected val futureSupervisor: FutureSupervisor,
    protected val exitOnFatalFailures: Boolean,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    protected val testOnlyInterceptor: PartyReplicationTestInterceptor,
)(implicit override val executionContext: ExecutionContext)
    extends PartyReplicationProcessor {
  protected val processorStore: SourceParticipantStore = InMemoryProcessorStore.sourceParticipant()

  // TODO(#22251): Make this configurable.
  private val contractsPerBatch = PositiveInt.two

  override def replicatedContractsCount: NonNegativeInt = processorStore.sentContractsCount

  override protected def name: String = "party-replication-source-processor"

  /** Once connected or reconnected, remember that the SP needs to be initialized by the TP.
    */
  override def onConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = execute("handle connect to TP") {
    super.onConnected().map(_ => processorStore.clearInitialContractOrdinalInclusive())
  }

  /** Handle instructions from the target participant
    */
  override def handlePayload(payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = execute("handle payload from TP") {
    notifyCounterParticipantAndPartyReplicatorOnError(for {
      messageFromTP <- EitherT.fromEither[FutureUnlessShutdown](
        PartyReplicationTargetParticipantMessage
          .fromByteString(protocolVersion, payload)
          .leftMap(_.message)
      )
      _ <- messageFromTP.instruction match {
        case PartyReplicationTargetParticipantMessage.SendAcsUpTo(maxOrdinal) =>
          handleSendAcsUpTo(maxOrdinal)
        case PartyReplicationTargetParticipantMessage.Initialize(minOrdinal) =>
          handleInitialize(minOrdinal)
      }
    } yield ())
  }

  private def handleSendAcsUpTo(maxContractOrdinalInclusive: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.debug(
      s"Source participant has received instruction to send up to contract ordinal $maxContractOrdinalInclusive"
    )
    for {
      // Check that the target participant has initialized this SP.
      _ <- EitherT.cond[FutureUnlessShutdown](
        processorStore.initialContractOrdinalInclusiveO.isDefined,
        (),
        "Target participant has not initialized source participant",
      )
      previousMaxOrdinalInclusive = processorStore.contractOrdinalToSendUpToExclusive.unwrap - 1
      // Check that the target participant is requesting higher contract ordinals.
      _ <- EitherT.cond[FutureUnlessShutdown](
        maxContractOrdinalInclusive.unwrap > previousMaxOrdinalInclusive,
        (),
        s"Target participant requested contract ordinals that are not strictly increasing $maxContractOrdinalInclusive compared to previous ordinal $previousMaxOrdinalInclusive",
      )
      _ = processorStore.setContractOrdinalToSendUpToExclusive(
        maxContractOrdinalInclusive + NonNegativeInt.one // +1 for inclusive to exclusive
      )
    } yield progressPartyReplication()
  }

  private def handleInitialize(
      initialContractOrdinalInclusive: NonNegativeInt
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(
      s"Source participant has received instruction initialize starting with contract $initialContractOrdinalInclusive"
    )
    val previousO = processorStore.getAndSetInitialContractOrdinalInclusive(
      initialContractOrdinalInclusive
    )
    // Reset the contract count and ordinal to clear what might have been requested in a previous connection.
    processorStore.setContractOrdinalToSendUpToExclusive(initialContractOrdinalInclusive)
    processorStore.resetContractsCount(initialContractOrdinalInclusive)
    EitherT.cond[FutureUnlessShutdown](
      previousO.isEmpty,
      (),
      s"Source participant has already been initialized with contract ordinal $previousO, asked to initialize again with $initialContractOrdinalInclusive",
    )
  }

  /** Single point of entry for progress monitoring and advancing of party replication states for
    * those states that are driven by the party replicator.
    */
  override def progressPartyReplication()(implicit traceContext: TraceContext): Unit =
    // Skip progress check if more than one other task is already queued that performs this same progress check or
    // is going to schedule a progress check.
    if (executionQueue.isAtMostOneTaskScheduled) {
      executeAsync(s"Respond to target participant if needed") {
        EitherTUtil.ifThenET(
          isChannelOpenForCommunication &&
            !hasEndOfACSBeenReached &&
            testOnlyInterceptor.onSourceParticipantProgress(
              processorStore
            ) == PartyReplicationTestInterceptor.Proceed &&
            processorStore.initialContractOrdinalInclusiveO.isDefined &&
            processorStore.sentContractsCount.unwrap < processorStore.contractOrdinalToSendUpToExclusive.unwrap - 1 // -1 for exclusive to inclusive
        )(
          respondToTargetParticipant()
        )
      }
    }

  private def respondToTargetParticipant()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    notifyCounterParticipantAndPartyReplicatorOnError {
      val fromInclusive = processorStore.sentContractsCount
      val toInclusive: NonNegativeInt = processorStore.contractOrdinalToSendUpToExclusive.map(_ - 1)
      logger.debug(
        s"Source participant looking up contract ordinals [${fromInclusive.unwrap},${toInclusive.unwrap}]"
      )
      for {
        contracts <- readContracts(fromInclusive, toInclusive)
        numContractsSending = contracts.flatten.size
        _ <- sendContracts(contracts, fromInclusive, numContractsSending)
        numSentInTotal = processorStore.increaseSentContractsCount(
          NonNegativeInt.tryCreate(numContractsSending)
        )
        // If there aren't enough contracts, send that we have reached the end of the ACS.
        _ <- EitherTUtil.ifThenET(numSentInTotal < toInclusive) {
          sendEndOfAcs(s"End of ACS after $numSentInTotal contracts").map(_ =>
            // Let the PartyReplicator know the SP is done, but let the TP, the channel owner, close the channel.
            onAcsFullyReplicated(traceContext)
          )
        }
      } yield ()
    }

  /** Reads contract batches from the ACS in a brute-force fashion via AcsInspection until
    * TODO(#24326) reads the ACS via the Ledger API.
    */
  private def readContracts(fromInclusive: NonNegativeInt, toInclusive: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Seq[NonEmpty[Seq[ActiveContractOld]]]] = {
    val contracts = List.newBuilder[ActiveContractOld]
    synchronizeWithClosing(s"Read ACS from ${fromInclusive.unwrap} to $toInclusive")(
      acsInspection
        .forEachVisibleActiveContract(
          psid.logical,
          Set(partyId.toLf),
          Some(TimeOfChange(activeAfter.immediateSuccessor)),
        ) { case (contractInst, reassignmentCounter) =>
          SerializableContract
            .fromLfFatContractInst(contractInst.inst)
            .bimap(
              err => SerializationIssue(psid.logical, contractInst.contractId, err),
              contract => {
                val stakeholdersHostedByTargetParticipant =
                  contract.metadata.stakeholders.intersect(otherPartiesHostedByTargetParticipant)
                if (stakeholdersHostedByTargetParticipant.isEmpty) {
                  contracts += ActiveContractOld
                    .create(psid.logical, contract, reassignmentCounter)(
                      protocolVersion
                    )
                } else {
                  // Skip contracts already hosted by the target participant.
                  logger.debug(
                    s"Skipping contract ${contract.contractId} as it is already hosted by ${stakeholdersHostedByTargetParticipant
                        .mkString(", ")} on the target participant between contract ordinals $fromInclusive and $toInclusive}"
                  )
                }
              },
            )
        }(traceContext, executionContext)
        .bimap(
          _.toString,
          _ =>
            contracts
              .result()
              .slice(fromInclusive.unwrap, toInclusive.unwrap + 1)
              .grouped(contractsPerBatch.unwrap)
              .toSeq
              .map(NonEmpty.from(_).getOrElse(throw new IllegalStateException("Grouping failed"))),
        )
    )
  }

  private def sendContracts(
      contractBatches: Seq[NonEmpty[Seq[ActiveContractOld]]],
      firstContractOrdinal: NonNegativeInt,
      numContractsSending: Int,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.debug(
      s"Source participant sending ${contractBatches.size} batches with contract ordinals from ${firstContractOrdinal.unwrap} to ${firstContractOrdinal.unwrap + numContractsSending - 1}"
    )
    val indexedContractBatches = contractBatches.zipWithIndex.map { case (batch, index) =>
      val fromInclusive = firstContractOrdinal +
        (NonNegativeInt.tryCreate(index) * contractsPerBatch.toNonNegative)
      val toInclusive = fromInclusive + NonNegativeInt.tryCreate(batch.size - 1)
      (batch, (fromInclusive, toInclusive))
    }
    MonadUtil.sequentialTraverse_(indexedContractBatches) {
      case (contracts, (fromInclusive, toInclusive)) =>
        val acsBatch = PartyReplicationSourceParticipantMessage(
          PartyReplicationSourceParticipantMessage.AcsBatch(contracts)
        )(
          PartyReplicationSourceParticipantMessage.protocolVersionRepresentativeFor(protocolVersion)
        )
        sendPayload(s"ACS batch from $fromInclusive to $toInclusive", acsBatch.toByteString)
    }
  }

  private def sendEndOfAcs(endOfStreamMessage: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(endOfStreamMessage)
    val endOfACS = PartyReplicationSourceParticipantMessage(
      PartyReplicationSourceParticipantMessage.EndOfACS
    )(
      PartyReplicationSourceParticipantMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
    for {
      _ <- sendPayload(endOfStreamMessage, endOfACS.toByteString)
    } yield {
      processorStore.setHasEndOfACSBeenReached()
      // Don't send an onComplete or close the channel yet. Let the TP as the owner of the channel close.
      // Having the target participant send the onComplete and initiate closing of the channel also avoids
      // flaky warnings in case the TP has not processed the EndOfACS message yet.
    }
  }

  override protected def hasEndOfACSBeenReached: Boolean = processorStore.hasEndOfACSBeenReached
}

object PartyReplicationSourceParticipantProcessor {
  def apply(
      psid: PhysicalSynchronizerId,
      partyId: PartyId,
      requestId: Hash,
      activeAt: CantonTimestamp,
      partiesHostedByTargetParticipant: Set[LfPartyId],
      acsInspection: AcsInspection,
      onAcsFullyReplicated: TraceContext => Unit,
      onError: String => Unit,
      onDisconnect: (String, TraceContext) => Unit,
      futureSupervisor: FutureSupervisor,
      exitOnFatalFailures: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      testInterceptor: PartyReplicationTestInterceptor =
        PartyReplicationTestInterceptor.AlwaysProceed,
  )(implicit executionContext: ExecutionContext): PartyReplicationSourceParticipantProcessor =
    new PartyReplicationSourceParticipantProcessor(
      psid,
      partyId,
      activeAt,
      partiesHostedByTargetParticipant,
      acsInspection,
      onAcsFullyReplicated,
      onError,
      onDisconnect,
      futureSupervisor,
      exitOnFatalFailures,
      timeouts,
      loggerFactory
        .append("synchronizerId", psid.toProtoPrimitive)
        .append("partyId", partyId.toProtoPrimitive)
        .append("requestId", requestId.toHexString),
      testInterceptor,
    )
}
