// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps

import java.util.concurrent.atomic.AtomicReference

/** InMemoryProcessorStore encapsulates the state of the source and target participants.
  */
object InMemoryProcessorStore {
  def sourceParticipant(): SourceParticipantStore = new SourceParticipantStore()

  def targetParticipant(): TargetParticipantStore = new TargetParticipantStore()
}

object SourceParticipantStore {

  /** The source participant (SP) tracks send-related contract ordinal watermarks in particular
    *   1. which contracts have been sent,
    *   1. up to which contract ordinal the SP is expected to send,
    *   1. once initialized, the initial contract ordinal according to the most recent
    *      initialization,
    *   1. whether the SP has confirmed that the end of the ACS has been reached.
    */
  private final case class SPState(
      sentContractsCount: NonNegativeInt,
      contractOrdinalToSendUpToExclusive: NonNegativeInt,
      initialContractOrdinalInclusiveO: Option[NonNegativeInt],
      hasEndOfACSBeenReached: Boolean = false,
  )
}

sealed trait PartyReplicationProcessorStore {

  /** Clear the initial contract ordinal to ensure the processor first initiates or waits for
    * initialization (e.g. on disconnect or reconnect) rather than taking any other premature action
    * (e.g. sending or processing non-initialization-related messages).
    */
  private[party] def clearInitialContractOrdinalInclusive(): Unit
}

final class SourceParticipantStore extends PartyReplicationProcessorStore {
  private val state = new AtomicReference[SourceParticipantStore.SPState](
    SourceParticipantStore.SPState(
      sentContractsCount = NonNegativeInt.zero,
      contractOrdinalToSendUpToExclusive = NonNegativeInt.zero,
      initialContractOrdinalInclusiveO = None,
    )
  )

  def sentContractsCount: NonNegativeInt = state.get().sentContractsCount

  /** Increase the number of sent contracts and return the increased sent contract count. */
  private[party] def increaseSentContractsCount(delta: NonNegativeInt): NonNegativeInt =
    state
      .updateAndGet(state =>
        state.copy(sentContractsCount = state.sentContractsCount.map(_ + delta.unwrap))
      )
      .sentContractsCount

  private[party] def resetContractsCount(count: NonNegativeInt): Unit =
    state.updateAndGet(_.copy(sentContractsCount = count)).discard

  private[party] def contractOrdinalToSendUpToExclusive: NonNegativeInt =
    state.get().contractOrdinalToSendUpToExclusive
  private[party] def setContractOrdinalToSendUpToExclusive(n: NonNegativeInt): Unit =
    state.updateAndGet(_.copy(contractOrdinalToSendUpToExclusive = n)).discard

  private[party] def initialContractOrdinalInclusiveO: Option[NonNegativeInt] =
    state.get().initialContractOrdinalInclusiveO
  override private[party] def clearInitialContractOrdinalInclusive(): Unit =
    state.updateAndGet(_.copy(initialContractOrdinalInclusiveO = None)).discard

  /** returns previous value after setting / updating */
  private[party] def getAndSetInitialContractOrdinalInclusive(
      ordinal: NonNegativeInt
  ): Option[NonNegativeInt] =
    state
      .getAndUpdate(_.copy(initialContractOrdinalInclusiveO = Some(ordinal)))
      .initialContractOrdinalInclusiveO

  private[party] def hasEndOfACSBeenReached: Boolean = state.get().hasEndOfACSBeenReached
  private[party] def setHasEndOfACSBeenReached(): Unit =
    state.updateAndGet(_.copy(hasEndOfACSBeenReached = true)).discard
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
  override private[party] def clearInitialContractOrdinalInclusive(): Unit =
    state.updateAndGet(_.copy(initialContractOrdinalInclusiveO = None)).discard
  private[party] def setInitialContractOrdinalInclusive(ordinal: NonNegativeInt): Unit =
    state
      .updateAndGet(_.copy(initialContractOrdinalInclusiveO = Some(ordinal)))
      .discard

  private[party] def hasEndOfACSBeenReached: Boolean = state.get().hasEndOfACSBeenReached
  private[party] def setHasEndOfACSBeenReached(): Unit =
    state.updateAndGet(_.copy(hasEndOfACSBeenReached = true)).discard
}
