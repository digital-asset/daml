// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import cats.Monad
import cats.syntax.functor.*
import com.daml.error.BaseError
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block
import com.digitalasset.canton.domain.block.BlockSequencerStateManager.HeadState
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  BlockUpdateClosureWithHeight,
  SequencerBlockStore,
}
import com.digitalasset.canton.domain.sequencing.integrations.state.EphemeralState
import com.digitalasset.canton.domain.sequencing.integrations.state.statemanager.MemberCounters
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencer
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.error.SequencerBaseError
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  CloseContext,
  FlagCloseableAsync,
  FutureUnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.pekkostreams.dispatcher.SubSource.RangeSource
import com.digitalasset.canton.sequencing.client.SequencerSubscriptionError
import com.digitalasset.canton.topology.{DomainId, Member, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.Done
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.Keep

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

/** Thrown if the ephemeral state does not match what is expected in the persisted store.
  * This is not expected to be able to occur, but if it does likely means that the
  * ephemeral state is inconsistent with the persisted state.
  * The sequencer should be restarted and logs verified to ensure that the persisted state is correct.
  */
class SequencerUnexpectedStateChange(message: String = "Sequencer state has unexpectedly changed")
    extends RuntimeException(message)

/** State manager for operating a sequencer using Blockchain based infrastructure (such as fabric or ethereum) */
trait BlockSequencerStateManagerBase extends FlagCloseableAsync {

  type CreateSubscription = Either[CreateSubscriptionError, Sequencer.EventSource]

  val maybeLowerTopologyTimestampBound: Option[CantonTimestamp]

  private[domain] def firstSequencerCounterServableForSequencer: SequencerCounter

  def getHeadState: HeadState

  /** Check whether a member is currently registered based on the latest state. */
  def isMemberRegistered(member: Member): Boolean

  /** Check whether a member is currently enabled based on the latest state. */
  def isMemberEnabled(member: Member): Boolean

  def handleBlock(
      updateClosure: BlockUpdateClosureWithHeight
  ): FutureUnlessShutdown[BlockEphemeralState]

  def handleLocalEvent(
      event: BlockSequencer.LocalEvent
  )(implicit traceContext: TraceContext): Future[Unit]

  def pruneLocalDatabase(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Wait for a member to be disabled on the underlying ledger */
  def waitForMemberToBeDisabled(member: Member): Future[Unit]

  /** Wait for the sequencer pruning request to have been processed and get the returned message */
  def waitForPruningToComplete(timestamp: CantonTimestamp): Future[String]

  /** Wait for the member's acknowledgement to have been processed */
  def waitForAcknowledgementToComplete(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def readEventsForMember(member: Member, startingAt: SequencerCounter)(implicit
      traceContext: TraceContext
  ): CreateSubscription
}

class BlockSequencerStateManager(
    protocolVersion: ProtocolVersion,
    domainId: DomainId,
    sequencerId: SequencerId,
    val store: SequencerBlockStore,
    enableInvariantCheck: Boolean,
    private val initialMemberCounters: MemberCounters,
    override val maybeLowerTopologyTimestampBound: Option[CantonTimestamp],
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, closeContext: CloseContext)
    extends BlockSequencerStateManagerBase
    with NamedLogging {

  import BlockSequencerStateManager.*

  private val memberRegistrationPromises = TrieMap[Member, Promise[CantonTimestamp]]()
  private val memberDisablementPromises = TrieMap[Member, Promise[Unit]]()
  private val sequencerPruningPromises = TrieMap[CantonTimestamp, Promise[String]]()
  private val memberAcknowledgementPromises =
    TrieMap[Member, NonEmpty[SortedMap[CantonTimestamp, Traced[Promise[Unit]]]]]()

  private val headState = new AtomicReference[HeadState]({
    import TraceContext.Implicits.Empty.*
    val headBlock =
      timeouts.unbounded.await(s"Reading the head of the $domainId sequencer state")(store.readHead)
    HeadState.fullyProcessed(headBlock)
  })

  private val countersSupportedAfter = new AtomicReference[MemberCounters](initialMemberCounters)

  private val dispatchers: TrieMap[Member, Dispatcher[SequencerCounter]] = TrieMap.empty

  override private[domain] def firstSequencerCounterServableForSequencer: SequencerCounter =
    countersSupportedAfter
      .get()
      .get(sequencerId)
      .fold(SequencerCounter.Genesis)(_ + 1)

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    dispatchers.values.toSeq.map(d =>
      AsyncCloseable(
        s"${this.getClass}: dispatcher $d",
        d.shutdown(),
        timeouts.shutdownShort,
      )
    )
  }

  override def getHeadState: HeadState = headState.get()

  override def isMemberRegistered(member: Member): Boolean =
    headState.get().chunk.ephemeral.registeredMembers.contains(member)

  /** Check whether a member is currently enabled based on the latest state. */
  override def isMemberEnabled(member: Member): Boolean =
    headState.get().chunk.ephemeral.status.members.exists(s => s.enabled && s.member == member)

  override def handleBlock(
      updateClosure: BlockUpdateClosureWithHeight
  ): FutureUnlessShutdown[BlockEphemeralState] = {
    implicit val traceContext: TraceContext = updateClosure.blockTraceContext
    closeContext.context.performUnlessClosingUSF("handleBlock") {

      val blockEphemeralState = {
        headState.get().blockEphemeralState
      }
      checkInvariantIfEnabled(blockEphemeralState)
      val height = updateClosure.height
      val lastBlockHeight = blockEphemeralState.latestBlock.height

      // TODO(M98 Tech-Debt Collection): consider validating that blocks with the same block height have the same contents
      // Skipping blocks we have processed before. Can occur when the read-path flowable is re-started but not all blocks
      // in the pipeline of the BlockSequencerStateManager have already been processed.
      if (height <= lastBlockHeight) {
        logger.debug(s"Skipping update with height $height since it was already processed. ")(
          traceContext
        )
        FutureUnlessShutdown.pure(blockEphemeralState)
      } else if (lastBlockHeight > block.UninitializedBlockHeight && height > lastBlockHeight + 1) {
        val msg =
          s"Received block of height $height while the last processed block only had height $lastBlockHeight. " +
            s"Expected to receive one block higher only."
        logger.error(msg)
        FutureUnlessShutdown.failed(new SequencerUnexpectedStateChange(msg))
      } else
        updateClosure
          .updateGenerator(blockEphemeralState)
          .flatMap(handleUpdate)
    }
  }

  override def handleLocalEvent(
      event: BlockSequencer.LocalEvent
  )(implicit traceContext: TraceContext): Future[Unit] = event match {
    case BlockSequencer.DisableMember(member) => locallyDisableMember(member)
    case BlockSequencer.Prune(timestamp) => pruneLocalDatabase(timestamp)
  }

  override def pruneLocalDatabase(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = for {
    msg <- store.prune(timestamp)
    initialCounters <- store.initialMemberCounters
  } yield {
    countersSupportedAfter.set(initialCounters)
    resolveSequencerPruning(timestamp, msg)
  }

  override def waitForMemberToBeDisabled(member: Member): Future[Unit] =
    memberDisablementPromises.getOrElseUpdate(member, Promise[Unit]()).future

  override def waitForPruningToComplete(timestamp: CantonTimestamp): Future[String] =
    sequencerPruningPromises.getOrElseUpdate(timestamp, Promise[String]()).future

  override def waitForAcknowledgementToComplete(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    memberAcknowledgementPromises
      .updateWith(member) {
        case None => Some(NonEmpty(SortedMap, timestamp -> Traced(Promise[Unit]())))
        case Some(promises) =>
          Some(
            if (promises.contains(timestamp)) promises
            else promises.updated(timestamp, Traced(Promise[Unit]()))
          )
      }
      .getOrElse(
        ErrorUtil.internalError(
          new NoSuchElementException(
            "The updateWith function returned None despite the update rule always returning a Some"
          )
        )
      )(timestamp)
      .value
      .future

  override def readEventsForMember(member: Member, startingAt: SequencerCounter)(implicit
      traceContext: TraceContext
  ): CreateSubscription = {
    logger.debug(
      s"Read events for member ${member} starting at ${startingAt}"
    )
    def checkCounterIsSupported(
        member: Member,
        startingAt: SequencerCounter,
    ): Either[CreateSubscriptionError, Unit] =
      countersSupportedAfter.get().get(member) match {
        case Some(counter) if startingAt <= counter =>
          logger.info(
            s"Given counter $startingAt for member $member is not supported. Counter has to be greater than $counter"
          )
          Left(CreateSubscriptionError.InvalidCounter(startingAt))
        case _ => Right(())
      }

    performUnlessClosing[CreateSubscription](functionFullName) {
      for {
        _ <- Either.cond(
          startingAt >= SequencerCounter.Genesis,
          (),
          CreateSubscriptionError.InvalidCounter(startingAt): CreateSubscriptionError,
        )
        _ <- Either.cond(
          !headState.get().chunk.ephemeral.status.disabledClients.members.contains(member),
          (),
          CreateSubscriptionError.MemberDisabled(member),
        )
        _ <- checkCounterIsSupported(member, startingAt)

      } yield {
        val dispatcher = getOrCreateDispatcher(member)
        dispatcher
          .startingAt(
            startingAt,
            RangeSource((s, e) => store.readRange(member, s, e).map(e => e.counter -> e)),
          )
          .map { case (_, event) =>
            if (event.isTombstone) {
              val err =
                s"Encountered tombstone ${event.counter} and ${event.timestamp} for $member"
              logger.warn(s"Terminating subscription due to: $err")(event.traceContext)
              Left(
                SequencerSubscriptionError.TombstoneEncountered.Error(err)
              )
            } else {
              Right(event)
            }
          }
          // If we run into a subscription error due to a tombstone, ensure the error is the last emitted entry
          .takeWhile(_.isRight, inclusive = true)
          .viaMat(KillSwitches.single)(Keep.right)
          .mapMaterializedValue(_ -> Future.successful(Done))

      }
    }.onShutdown {
      logger.info(s"${this.getClass} is shutting down, not reading events")
      Left(CreateSubscriptionError.ShutdownError): CreateSubscription
    }
  }

  private def locallyDisableMember(
      member: Member
  )(implicit traceContext: TraceContext): Future[Unit] =
    store
      .partialBlockUpdate(
        newMembers = Map(),
        events = Seq(),
        acknowledgments = Map(),
        membersDisabled = Seq(member),
        inFlightAggregationUpdates = Map(),
        trafficState = Map(),
      )
      .map { _ =>
        import monocle.macros.syntax.lens.*
        val head = getHeadState
        val memebersMap = head.chunk.ephemeral.status.membersMap
        val newHead = head
          .focus(_.chunk.ephemeral.status.membersMap)
          .replace(memebersMap.updated(member, memebersMap(member).copy(enabled = false)))
        updateHeadState(head, newHead)
        resolveWaitingForMemberDisablement(member)
      }

  private def updateMemberCounterSupportedAfter(member: Member, counter: SequencerCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    store
      .updateMemberCounterSupportedAfter(member, counter)
      .map(_ =>
        countersSupportedAfter.getAndUpdate { previousCounters =>
          if (previousCounters.get(member).exists(_ >= counter))
            previousCounters
          else
            previousCounters + (member -> counter)
        }.discard
      )

  private def handleChunkUpdate(update: ChunkUpdate)(implicit
      batchTraceContext: TraceContext
  ): Future[Unit] = {
    val priorHead = headState.get()
    val priorState = priorHead.chunk
    val chunkNumber = priorState.chunkNumber + 1
    assert(
      update.newMembers.values.forall(_ >= priorState.lastTs),
      s"newMembers in chunk $chunkNumber should be assigned a timestamp after the timestamp of the previous chunk or block",
    )
    assert(
      update.signedEvents.view.flatMap(_.values.map(_.timestamp)).forall(_ > priorState.lastTs),
      s"Events in chunk $chunkNumber have timestamp lower than in the previous chunk or block",
    )
    assert(
      update.lastTopologyClientTimestamp.forall(last =>
        priorState.latestTopologyClientTimestamp.forall(_ < last)
      ),
      s"The last topology client timestamp ${update.lastTopologyClientTimestamp} in chunk $chunkNumber must be later than the previous chunk's or block's latest topology client timestamp at ${priorState.latestTopologyClientTimestamp}",
    )

    def checkFirstSequencerCounters: Boolean = {
      val firstSequencerCounterByMember =
        update.signedEvents
          .map(_.forgetNE.fmap(_.counter))
          .foldLeft(Map.empty[Member, SequencerCounter])(
            MapsUtil.mergeWith(_, _)((first, _) => first)
          )
      firstSequencerCounterByMember.forall { case (member, firstSequencerCounter) =>
        priorState.ephemeral.heads.getOrElse(member, SequencerCounter.Genesis - 1L) ==
          firstSequencerCounter - 1L
      }
    }

    assert(
      checkFirstSequencerCounters,
      s"There is a gap in sequencer counters between chunks $chunkNumber and the previous chunk or block.",
    )

    val lastTs =
      (update.signedEvents.view.flatMap(_.values.map(_.timestamp)) ++
        update.newMembers.values).maxOption.getOrElse(priorState.lastTs)
    val newState = ChunkState(
      chunkNumber,
      update.state,
      lastTs,
      update.lastTopologyClientTimestamp.orElse(priorState.latestTopologyClientTimestamp),
    )

    logger.debug(s"Adding block updates for chunk $chunkNumber to store")
    for {
      _ <- store.partialBlockUpdate(
        newMembers = update.newMembers,
        events = update.signedEvents,
        acknowledgments = update.acknowledgements,
        membersDisabled = update.membersDisabled,
        inFlightAggregationUpdates = update.inFlightAggregationUpdates,
        update.state.trafficState,
      )
      _ <- MonadUtil.sequentialTraverse[(Member, SequencerCounter), Future, Unit](
        update.signedEvents
          .flatMap(_.toSeq)
          .collect {
            case (member, tombstone) if tombstone.isTombstone => member -> tombstone.counter
          }
      ) { case (member, counter) => updateMemberCounterSupportedAfter(member, counter) }
      _ <- // Advance the supported counters before we delete the data of the old counters
        if (update.pruningRequests.nonEmpty)
          store.initialMemberCounters.map(initial => countersSupportedAfter.set(initial))
        else Future.unit
      _ <- Future.sequence(update.pruningRequests.map(_.withTraceContext {
        pruneTraceContext => ts =>
          logger.debug("Performing sequencer pruning on local state")(pruneTraceContext)
          store.prune(ts)(pruneTraceContext).map(resolveSequencerPruning(ts, _))
      }))
    } yield {
      // head state update must happen before member counters are updated
      // as otherwise, if we have a registration in between counter-signalling and head-state,
      // the dispatcher will be initialised with the old head state but not be notified about
      // a change.
      updateHeadState(priorHead, priorHead.copy(chunk = newState))
      signalMemberCountersToDispatchers(newState.ephemeral)
      resolveWaitingForNewMembers(newState.ephemeral)
      resolveWaitingForMemberDisablement(newState.ephemeral)
      update.acknowledgements.foreach { case (member, timestamp) =>
        resolveAcknowledgements(member, timestamp)
      }
      update.invalidAcknowledgements.foreach { case (member, timestamp, error) =>
        invalidAcknowledgement(member, timestamp, error)
      }
    }
  }

  private def handleUpdate(update: BlockUpdates)(implicit
      blockTraceContext: TraceContext
  ): FutureUnlessShutdown[BlockEphemeralState] = {

    def handleComplete(newBlock: BlockInfo): Future[BlockEphemeralState] = {
      val priorHead = headState.get
      val chunkState = priorHead.chunk

      assert(
        chunkState.lastTs <= newBlock.lastTs,
        s"The block's last timestamp must be at least the last timestamp of the last chunk",
      )
      assert(
        chunkState.latestTopologyClientTimestamp <= newBlock.latestTopologyClientTimestamp,
        s"The block's latest topology client timestamp must be at least the last chunk's latest topology client timestamp",
      )

      val newState = BlockEphemeralState(
        newBlock,
        // We can expire the cached in-memory in-flight aggregations,
        // but we must not expire the persisted aggregations
        // because we still need them for computing a snapshot
        chunkState.ephemeral.evictExpiredInFlightAggregations(newBlock.lastTs),
      )
      checkInvariantIfEnabled(newState)
      val newHead = HeadState.fullyProcessed(newState)
      for {
        _ <- store.finalizeBlockUpdate(newBlock)
      } yield {
        updateHeadState(priorHead, newHead)
        newState
      }
    }

    def step(
        updates: BlockUpdates
    ): FutureUnlessShutdown[Either[BlockUpdates, BlockEphemeralState]] = {
      updates match {
        case PartialBlockUpdate(chunk, continuation) =>
          FutureUnlessShutdown
            .outcomeF(handleChunkUpdate(chunk))
            .flatMap((_: Unit) => continuation.map(Left(_)))
        case CompleteBlockUpdate(block) =>
          FutureUnlessShutdown.outcomeF(handleComplete(block)).map(Right(_))
      }
    }

    Monad[FutureUnlessShutdown].tailRecM(update)(step)
  }

  private def updateHeadState(prior: HeadState, next: HeadState)(implicit
      traceContext: TraceContext
  ): Unit = {
    if (!headState.compareAndSet(prior, next)) {
      // The write flow should not call this method concurrently so this situation should never happen.
      // If it does, this means that the ephemeral state has been updated since this update was generated,
      // and that the persisted state is now likely inconsistent.
      // throw exception to shutdown the sequencer write flow as we can not continue.
      ErrorUtil.internalError(new SequencerUnexpectedStateChange)
    }
  }

  private def signalMemberCountersToDispatchers(
      newState: EphemeralState
  ): Unit = {
    dispatchers.toList.foreach { case (member, dispatcher) =>
      newState.heads.get(member) match {
        case Some(counter) =>
          dispatcher.signalNewHead(counter + 1L)
        case None =>
      }
    }
  }

  private def resolveWaitingForNewMembers(newState: EphemeralState): Unit = {
    // if any members that we're waiting to see are now registered members, complete those promises.
    newState.status.members foreach { registeredMember =>
      memberRegistrationPromises.remove(registeredMember.member) foreach { promise =>
        promise.success(registeredMember.registeredAt)
      }
    }
  }

  private def resolveWaitingForMemberDisablement(newState: EphemeralState): Unit = {
    // if any members that we're waiting to see disabled are now disabled members, complete those promises.
    memberDisablementPromises.keys
      .map(newState.status.membersMap(_))
      .filterNot(_.enabled)
      .map(_.member) foreach (resolveWaitingForMemberDisablement)
  }

  private def resolveWaitingForMemberDisablement(disabledMember: Member): Unit =
    memberDisablementPromises.remove(disabledMember) foreach { promise => promise.success(()) }

  private def resolveSequencerPruning(timestamp: CantonTimestamp, pruningMsg: String): Unit = {
    sequencerPruningPromises.remove(timestamp) foreach { promise => promise.success(pruningMsg) }
  }

  /** Resolves all outstanding acknowledgements up to the given timestamp.
    * Unlike for resolutions of other requests, we resolve also all earlier acknowledgements,
    * because this mimics the effect of the acknowledgement: all earlier acknowledgements are irrelevant now.
    */
  private def resolveAcknowledgements(member: Member, upToInclusive: CantonTimestamp)(implicit
      tc: TraceContext
  ): Unit = {
    // Use a `var` here to obtain the previous value associated with the `member`,
    // as `updateWith` returns the new value. We could implement our own version of `updateWith` instead,
    // but we'd rely on internal Scala collections API for this.
    //
    // Don't complete the promises inside the `updateWith` function
    // as this is a side effect and the function may be evaluated several times.
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var previousPromises: Option[SortedMap[CantonTimestamp, Traced[Promise[Unit]]]] = None
    logger.debug(s"Resolving an acknowledgment for member $member")
    memberAcknowledgementPromises
      .updateWith(member) { previous =>
        previousPromises = previous
        previous match {
          case None => None
          case Some(promises) =>
            val remaining = promises.dropWhile { case (timestamp, _) =>
              timestamp <= upToInclusive
            }
            NonEmpty.from(remaining)
        }
      }
      .discard
    previousPromises
      .getOrElse(SortedMap.empty[CantonTimestamp, Traced[Promise[Unit]]])
      .takeWhile { case (timestamp, _) => timestamp <= upToInclusive }
      .foreach { case (_, tracedPromise) =>
        tracedPromise.value.success(())
      }
  }

  /** Complete the acknowledgement promise for `member` and `ackTimestamp` with an error
    */
  private def invalidAcknowledgement(
      member: Member,
      ackTimestamp: CantonTimestamp,
      error: BaseError,
  ): Unit = {
    // Use a `var` here to obtain the previous value associated with the `member`,
    // as `updateWith` returns the new value. We could implement our own version of `updateWith` instead,
    // but we'd rely on internal Scala collections API for this.
    //
    // Don't complete the promises inside the `updateWith` function
    // as this is a side effect and the function may be evaluated several times.
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var previousPromises: Option[SortedMap[CantonTimestamp, Traced[Promise[Unit]]]] = None
    memberAcknowledgementPromises
      .updateWith(member) { previous =>
        previousPromises = previous
        previous match {
          case None => None
          case Some(promises) =>
            if (promises.contains(ackTimestamp)) {
              NonEmpty.from(promises.removed(ackTimestamp))
            } else Some(promises)
        }
      }
      .discard
    previousPromises
      .getOrElse(SortedMap.empty[CantonTimestamp, Traced[Promise[Unit]]])
      .get(ackTimestamp)
      .foreach(_.withTraceContext { implicit traceContext => promise =>
        promise.failure(SequencerBaseError.asGrpcError(error))
      })

  }

  private def getOrCreateDispatcher(
      member: Member
  )(implicit traceContext: TraceContext): Dispatcher[SequencerCounter] =
    dispatchers.get(member) match {
      case Some(existing) => existing
      case None => createDispatcher(member)
    }

  private def createDispatcher(
      member: Member
  )(implicit traceContext: TraceContext): Dispatcher[SequencerCounter] = {

    def makeDispatcher() = {
      // The dispatcher head uses the index of the next message, not the current like we use in the sequencer
      // and then add 1 to the counter head when setting the new dispatcher index
      // So empty means we pass Genesis counter (0), everything else is counter + 1 (so the first msg is signalled 1)
      val head =
        headState.get().chunk.ephemeral.heads.get(member) match {
          case Some(counter) =>
            logger.debug(
              s"Creating dispatcher for [$member] from head=${counter + 1L}"
            )
            counter + 1L
          case None =>
            logger.debug(
              s"Creating dispatcher for [$member] from genesis (${SequencerCounter.Genesis})"
            )
            SequencerCounter.Genesis
        }
      Dispatcher(
        name = show"${sequencerId.uid.id}-$member",
        zeroIndex = SequencerCounter.Genesis - 1,
        headAtInitialization = head,
      )
    }

    val dispatcher = blocking {
      // Avoids a race condition where the dispatcher is queried between creating and assigning to the map.
      dispatchers.synchronized {
        dispatchers.getOrElseUpdate(member, makeDispatcher())
      }
    }
    // now, there is a slight chance that the head changed and the dispatchers were notified
    // while we were updating the dispatchers
    // the order is: updateHead, signalDispatchers
    // the race is therefore makeDispatcher, updateHead, signalDispatchers, update dispatchers
    // to avoid a blocking lock, we just poke the newly generated dispatcher if necessary
    headState.get().chunk.ephemeral.heads.get(member).foreach { counter =>
      if (dispatcher.getHead() != counter + 1) {
        dispatcher.signalNewHead(counter + 1)
      }
    }
    dispatcher
  }

  private def checkInvariantIfEnabled(
      blockState: BlockEphemeralState
  )(implicit traceContext: TraceContext): Unit =
    if (enableInvariantCheck) blockState.checkInvariant()
}

object BlockSequencerStateManager {

  def apply(
      protocolVersion: ProtocolVersion,
      domainId: DomainId,
      sequencerId: SequencerId,
      store: SequencerBlockStore,
      enableInvariantCheck: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[BlockSequencerStateManager] =
    for {
      counters <- store.initialMemberCounters
      maybeLowerTopologyTimestampBound <- store.getInitialTopologySnapshotTimestamp
    } yield {

      new BlockSequencerStateManager(
        protocolVersion = protocolVersion,
        domainId = domainId,
        sequencerId = sequencerId,
        store = store,
        enableInvariantCheck = enableInvariantCheck,
        initialMemberCounters = counters,
        maybeLowerTopologyTimestampBound = maybeLowerTopologyTimestampBound,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
      )
    }

  /** Keeps track of the accumulated state changes by processing chunks of updates from a block
    *
    * @param chunkNumber The sequence number of the chunk
    */
  final case class ChunkState(
      chunkNumber: Long,
      ephemeral: EphemeralState,
      lastTs: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
  )

  object ChunkState {
    val initialChunkCounter = 0L

    def initial(block: BlockEphemeralState): ChunkState =
      ChunkState(
        initialChunkCounter,
        block.state,
        block.latestBlock.lastTs,
        block.latestBlock.latestTopologyClientTimestamp,
      )
  }

  /** The head state is updated after each chunk.
    *
    * @param block Describes the state after the latest block that was fully processed.
    * @param chunk Describes the state after the last chunk of the block that is currently being processed.
    *              When the latest block is fully processed, but no chunks of the next block,
    *              then this is `ChunkState.initial`
    *              based on the last block's [[com.digitalasset.canton.domain.block.data.BlockEphemeralState]].
    */
  final case class HeadState(
      block: BlockInfo,
      chunk: ChunkState,
  ) {
    def blockEphemeralState(implicit
        loggingContext: ErrorLoggingContext
    ): BlockEphemeralState = {
      ErrorUtil.requireState(
        chunk.chunkNumber == ChunkState.initialChunkCounter,
        s"Cannot construct a BlockEphemeralState if there are partial block updates from ${chunk.chunkNumber} chunks.",
      )
      BlockEphemeralState(block, chunk.ephemeral)
    }
  }

  object HeadState {
    def fullyProcessed(block: BlockEphemeralState): HeadState =
      HeadState(block.latestBlock, ChunkState.initial(block))
  }
}
