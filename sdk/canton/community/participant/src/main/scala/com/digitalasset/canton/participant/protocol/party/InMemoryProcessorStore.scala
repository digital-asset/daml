// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference

/** InMemoryProcessorStore encapsulates the state of the source and target participants.
  */
object InMemoryProcessorStore {
  def sourceParticipant(
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  ): SourceParticipantStore =
    new SourceParticipantStore(loggerFactory, timeouts)

  def targetParticipant(): TargetParticipantStore = new TargetParticipantStore()
}

object SourceParticipantStore {

  /** The source participant (SP) tracks send-related contract ordinal watermarks in particular
    *   1. which contracts have been sent,
    *   1. up to which contract ordinal the SP is expected to send,
    *   1. once initialized, the initial contract ordinal according to the most recent
    *      initialization,
    *   1. whether the SP has confirmed that the end of the ACS has been reached.
    *   1. the acs reader holding the pekko source that consumes the ACS via the ledger API.
    *   1. a counter for logging to distinguish multiple ACS readers created across multiple
    *      initializations.
    */
  private final case class SPState(
      sentContractsCount: NonNegativeInt,
      contractOrdinalToSendUpToExclusive: NonNegativeInt,
      initialContractOrdinalInclusiveO: Option[NonNegativeInt],
      hasEndOfACSBeenReached: Boolean = false,
      acsReaderO: Option[PartyReplicationAcsReader] = None,
      nextAcsReaderId: Int = 0,
  )
}

sealed trait PartyReplicationProcessorStore {

  /** Handles resetting of connection-specific state upon connect, disconnect, reconnect, or close:
    *   - Clear the initial contract ordinal to ensure the processor first initiates or waits for
    *     initialization (e.g. on disconnect or reconnect) rather than taking any other premature
    *     action (e.g. sending or processing non-initialization-related messages).
    *   - Close any connection-specific resources such as the ACS reader.
    */
  private[party] def resetConnection(): Unit
}

final class SourceParticipantStore(
    protected val loggerFactory: NamedLoggerFactory,
    protected val timeouts: ProcessingTimeout,
) extends PartyReplicationProcessorStore
    with FlagCloseable
    with NamedLogging {
  private val state = new AtomicReference[SourceParticipantStore.SPState](
    SourceParticipantStore.SPState(
      sentContractsCount = NonNegativeInt.zero,
      contractOrdinalToSendUpToExclusive = NonNegativeInt.zero,
      initialContractOrdinalInclusiveO = None,
    )
  )

  def initializeSourceParticipantState(
      initialContractOrdinalInclusive: NonNegativeInt,
      createAcsReader: (SourceParticipantStore, NamedLoggerFactory) => PartyReplicationAcsReader,
  )(implicit traceContext: TraceContext): Either[String, Unit] =
    Either
      .cond(
        initialContractOrdinalInclusiveO.isEmpty,
        (),
        s"Source participant has already been initialized with contract ordinal $initialContractOrdinalInclusiveO, asked to initialize again with $initialContractOrdinalInclusive",
      )
      .map { _ =>
        // Reinitialize the state using the initialization starting ordinal.
        val acsReaderId = state
          .updateAndGet(before =>
            SourceParticipantStore.SPState(
              initialContractOrdinalInclusiveO = Some(initialContractOrdinalInclusive),
              sentContractsCount = initialContractOrdinalInclusive,
              contractOrdinalToSendUpToExclusive = initialContractOrdinalInclusive,
              nextAcsReaderId = before.nextAcsReaderId + 1,
            )
          )
          .nextAcsReaderId

        // Create the ACS reader in a separate step.
        val acsReader = createAcsReader(this, loggerFactory.append("reader", acsReaderId.toString))
        logger.info(
          s"Initialized ACS reader $acsReaderId with initial contract ordinal $initialContractOrdinalInclusive"
        )
        state.updateAndGet(_.copy(acsReaderO = Some(acsReader))).discard
      }

  def sentContractsCount: NonNegativeInt = state.get().sentContractsCount

  /** Increase the number of sent contracts and return the increased sent contract count. */
  private[party] def increaseSentContractsCount(delta: NonNegativeInt): NonNegativeInt =
    state
      .updateAndGet(state =>
        state.copy(sentContractsCount = state.sentContractsCount.map(_ + delta.unwrap))
      )
      .sentContractsCount

  private[party] def contractOrdinalToSendUpToExclusive: NonNegativeInt =
    state.get().contractOrdinalToSendUpToExclusive
  private[party] def setContractOrdinalToSendUpToExclusive(n: NonNegativeInt): Unit =
    state.updateAndGet(_.copy(contractOrdinalToSendUpToExclusive = n)).discard

  private[party] def initialContractOrdinalInclusiveO: Option[NonNegativeInt] =
    state.get().initialContractOrdinalInclusiveO
  override private[party] def resetConnection(): Unit =
    state
      .getAndUpdate(_.copy(initialContractOrdinalInclusiveO = None, acsReaderO = None))
      .acsReaderO
      .foreach(_.close())

  private[party] def hasEndOfACSBeenReached: Boolean = state.get().hasEndOfACSBeenReached
  private[party] def setHasEndOfACSBeenReached(): Unit =
    state.updateAndGet(_.copy(hasEndOfACSBeenReached = true)).discard

  private[party] def acsReaderO: Option[PartyReplicationAcsReader] = state.get().acsReaderO
}

object TargetParticipantStore {

  /** The target participant (TP) tracks receive-related contract counts in particular:
    *   1. how many contracts have been requested,
    *   1. how many contracts have been processed,
    *   1. the next repair counter to use for publishing the next ACS batch to the indexer, and
    *   1. once initialized, the initial contract ordinal according to the most recent
    *      initialization.
    *   1. whether the TP has been notified that end of the ACS has been reached.
    */
  private final case class TPState(
      requestedContractsCount: NonNegativeInt,
      processedContractsCount: NonNegativeInt,
      nextRepairCounter: RepairCounter,
      initialContractOrdinalInclusiveO: Option[NonNegativeInt],
      hasEndOfACSBeenReached: Boolean,
  )
}

final class TargetParticipantStore extends PartyReplicationProcessorStore {
  private val state = new AtomicReference[TargetParticipantStore.TPState](
    TargetParticipantStore.TPState(
      requestedContractsCount = NonNegativeInt.zero,
      processedContractsCount = NonNegativeInt.zero,
      nextRepairCounter = RepairCounter.Genesis,
      initialContractOrdinalInclusiveO = None,
      hasEndOfACSBeenReached = false,
    )
  )

  private[party] def requestedContractsCount: NonNegativeInt = state.get().requestedContractsCount
  private[party] def setRequestedContractsCount(count: NonNegativeInt): Unit =
    state
      .updateAndGet(_.copy(requestedContractsCount = count))
      .discard

  def processedContractsCount: NonNegativeInt = state.get().processedContractsCount

  /** Increase the number of processed contracts and return the increased processed contract count.
    */
  private[party] def increaseProcessedContractsCount(delta: PositiveInt): NonNegativeInt =
    state
      .updateAndGet(state =>
        state.copy(processedContractsCount = state.processedContractsCount.map(_ + delta.unwrap))
      )
      .processedContractsCount

  private[party] def getAndIncrementRepairCounter(): RepairCounter =
    state
      .updateAndGet(state => state.copy(nextRepairCounter = state.nextRepairCounter + 1))
      .nextRepairCounter

  private[party] def initialContractOrdinalInclusiveO: Option[NonNegativeInt] =
    state.get().initialContractOrdinalInclusiveO
  override private[party] def resetConnection(): Unit =
    state.updateAndGet(_.copy(initialContractOrdinalInclusiveO = None)).discard
  private[party] def setInitialContractOrdinalInclusive(ordinal: NonNegativeInt): Unit =
    state
      .updateAndGet(_.copy(initialContractOrdinalInclusiveO = Some(ordinal)))
      .discard

  private[party] def hasEndOfACSBeenReached: Boolean = state.get().hasEndOfACSBeenReached
  private[party] def setHasEndOfACSBeenReached(): Unit =
    state.updateAndGet(_.copy(hasEndOfACSBeenReached = true)).discard
}
