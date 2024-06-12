// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import cats.data.{EitherT, Nested}
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block
import com.digitalasset.canton.domain.block.BlockSequencerStateManager.HeadState
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  EphemeralState,
  SequencerBlockStore,
}
import com.digitalasset.canton.domain.block.update.BlockUpdateGenerator.BlockChunk
import com.digitalasset.canton.domain.block.update.{
  BlockUpdate,
  BlockUpdateGenerator,
  ChunkUpdate,
  CompleteBlockUpdate,
  LocalBlockUpdate,
  OrderedBlockUpdate,
  SignedChunkEvents,
  UnsignedChunkEvents,
}
import com.digitalasset.canton.domain.sequencing.integrations.state.statemanager.MemberCounters
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencer
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.domain.sequencing.sequencer.{Sequencer, SequencerIntegration}
import com.digitalasset.canton.error.BaseAlarm
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.pekkostreams.dispatcher.SubSource.RangeSource
import com.digitalasset.canton.sequencing.client.SequencerSubscriptionError
import com.digitalasset.canton.topology.{DomainId, Member, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, LoggerUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Flow, Keep}
import org.apache.pekko.{Done, NotUsed}

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

  /** Flow to turn [[com.digitalasset.canton.domain.block.BlockEvents]] of one block
    * into a series of [[update.OrderedBlockUpdate]]s
    * that are to be persisted subsequently using [[applyBlockUpdate]].
    */
  def processBlock(
      bug: BlockUpdateGenerator
  ): Flow[BlockEvents, Traced[OrderedBlockUpdate[SignedChunkEvents]], NotUsed]

  /** Persists the [[update.BlockUpdate]]s and completes the waiting RPC calls
    * as necessary.
    */
  def applyBlockUpdate(
      dbSequencerIntegration: SequencerIntegration
  ): Flow[Traced[BlockUpdate[SignedChunkEvents]], Traced[CantonTimestamp], NotUsed]

  /** Wait for a member to be disabled on the underlying ledger */
  def waitForMemberToBeDisabled(member: Member): Future[Unit]

  /** Wait for the sequencer pruning request to have been processed and get the returned message */
  def waitForPruningToComplete(timestamp: CantonTimestamp): (Boolean, Future[Unit])

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
    rateLimitManager: SequencerRateLimitManager,
    unifiedSequencer: Boolean,
)(implicit executionContext: ExecutionContext)
    extends BlockSequencerStateManagerBase
    with NamedLogging {

  import BlockSequencerStateManager.*

  private val memberDisablementPromises = TrieMap[Member, Promise[Unit]]()
  private val sequencerPruningPromises = TrieMap[CantonTimestamp, Promise[Unit]]()
  private val memberAcknowledgementPromises =
    TrieMap[Member, NonEmpty[SortedMap[CantonTimestamp, Traced[Promise[Unit]]]]]()

  private val headState = new AtomicReference[HeadState]({
    import TraceContext.Implicits.Empty.*
    val headBlock =
      timeouts.unbounded.await(s"Reading the head of the $domainId sequencer state")(store.readHead)
    logger.debug(s"Initialized the block sequencer with head block ${headBlock.latestBlock}")
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
  override def isMemberEnabled(member: Member): Boolean = {
    val headStatus = headState.get().chunk.ephemeral.status
    headStatus.membersMap.contains(member) && !headStatus.disabledMembers.contains(member)
  }

  override def processBlock(
      bug: BlockUpdateGenerator
  ): Flow[BlockEvents, Traced[OrderedBlockUpdate[SignedChunkEvents]], NotUsed] = {
    val head = getHeadState
    val bugState = {
      import TraceContext.Implicits.Empty.*
      bug.internalStateFor(head.blockEphemeralState)
    }
    Flow[BlockEvents]
      .via(checkBlockHeight(head.block.height))
      .via(chunkBlock(bug))
      .via(processChunk(bug)(bugState))
      .async
      .via(signEvents(bug))
  }

  private def checkBlockHeight(
      initialHeight: Long
  ): Flow[BlockEvents, Traced[BlockEvents], NotUsed] =
    Flow[BlockEvents].statefulMapConcat(() => {
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      var currentBlockHeight = initialHeight
      blockEvents => {
        val height = blockEvents.height

        // TODO(M98 Tech-Debt Collection): consider validating that blocks with the same block height have the same contents
        // Skipping blocks we have processed before. Can occur when the read-path flowable is re-started but not all blocks
        // in the pipeline of the BlockSequencerStateManager have already been processed.
        if (height <= currentBlockHeight) {
          noTracingLogger.debug(
            s"Skipping update with height $height since it was already processed. "
          )
          Seq.empty
        } else if (
          currentBlockHeight > block.UninitializedBlockHeight && height > currentBlockHeight + 1
        ) {
          val msg =
            s"Received block of height $height while the last processed block only had height $currentBlockHeight. " +
              s"Expected to receive one block higher only."
          noTracingLogger.error(msg)
          throw new SequencerUnexpectedStateChange(msg)
        } else {
          implicit val traceContext: TraceContext = TraceContext.ofBatch(blockEvents.events)(logger)
          // Set the current block height to the new block's height instead of + 1 of the previous value
          // so that we support starting from an arbitrary block height

          logger.debug(
            s"Processing block $height with ${blockEvents.events.size} block events.${blockEvents.events
                .map(_.value)
                .collectFirst { case LedgerBlockEvent.Send(timestamp, _, _) =>
                  s" First timestamp in block: $timestamp"
                }
                .getOrElse("")}"
          )
          currentBlockHeight = height
          Seq(Traced(blockEvents))
        }
      }
    })

  private def chunkBlock(
      bug: BlockUpdateGenerator
  ): Flow[Traced[BlockEvents], Traced[BlockChunk], NotUsed] =
    Flow[Traced[BlockEvents]].mapConcat(_.withTraceContext { implicit traceContext => blockEvents =>
      bug.chunkBlock(blockEvents).map(Traced(_))
    })

  private def processChunk(bug: BlockUpdateGenerator)(
      initialState: bug.InternalState
  ): Flow[Traced[BlockChunk], Traced[OrderedBlockUpdate[UnsignedChunkEvents]], NotUsed] = {
    implicit val traceContext: TraceContext = TraceContext.empty
    Flow[Traced[BlockChunk]].statefulMapAsyncUSAndDrain(initialState) { (state, tracedChunk) =>
      implicit val traceContext: TraceContext = tracedChunk.traceContext
      tracedChunk.traverse(blockChunk => Nested(bug.processBlockChunk(state, blockChunk))).value
    }
  }

  private def signEvents(bug: BlockUpdateGenerator): Flow[
    Traced[OrderedBlockUpdate[UnsignedChunkEvents]],
    Traced[OrderedBlockUpdate[SignedChunkEvents]],
    NotUsed,
  ] = {
    implicit val traceContext: TraceContext = TraceContext.empty
    Flow[Traced[OrderedBlockUpdate[UnsignedChunkEvents]]]
      .mapAsyncAndDrainUS(parallelism = chunkSigningParallelism)(
        _.traverse {
          case chunk: ChunkUpdate[UnsignedChunkEvents] =>
            lazy val signEvents = chunk.events
              .parTraverse(bug.signChunkEvents)
              .map(signed => chunk.copy(events = signed))
            LoggerUtil.clueF(s"Signing ${chunk.events.size} events")(signEvents.unwrap).discard
            signEvents
          case complete: CompleteBlockUpdate => FutureUnlessShutdown.pure(complete)
        }
      )
  }

  override def applyBlockUpdate(
      dbSequencerIntegration: SequencerIntegration
  ): Flow[Traced[BlockUpdate[SignedChunkEvents]], Traced[CantonTimestamp], NotUsed] = {
    implicit val traceContext = TraceContext.empty
    Flow[Traced[BlockUpdate[SignedChunkEvents]]].statefulMapAsync(getHeadState) {
      (priorHead, update) =>
        implicit val traceContext = update.traceContext
        val currentBlockNumber = priorHead.block.height + 1
        val fut = update.value match {
          case LocalBlockUpdate(local) =>
            handleLocalEvent(priorHead, local)(traceContext)
          case chunk: ChunkUpdate[SignedChunkEvents] =>
            val chunkNumber = priorHead.chunk.chunkNumber + 1
            LoggerUtil.clueF(
              s"Adding block updates for chunk $chunkNumber for block $currentBlockNumber. " +
                s"Contains ${chunk.events.size} events, ${chunk.acknowledgements.size} acks, ${chunk.newMembers.size} new members, " +
                s"and ${chunk.inFlightAggregationUpdates.size} in-flight aggregation updates"
            )(handleChunkUpdate(priorHead, chunk, dbSequencerIntegration)(traceContext))
          case complete: CompleteBlockUpdate =>
            LoggerUtil.clueF(
              s"Storing completion of block $currentBlockNumber"
            )(handleComplete(priorHead, complete.block)(traceContext))
        }
        fut.map(newHead => newHead -> Traced(newHead.block.lastTs))
    }
  }

  @VisibleForTesting
  private[domain] def handleLocalEvent(
      priorHead: HeadState,
      event: BlockSequencer.LocalEvent,
  )(implicit traceContext: TraceContext): Future[HeadState] = event match {
    case BlockSequencer.DisableMember(member) => locallyDisableMember(priorHead, member)
    case BlockSequencer.UpdateInitialMemberCounters(timestamp) =>
      updateInitialMemberCounters(timestamp).map { (_: Unit) =>
        // Pruning does not change the head state
        priorHead
      }
  }

  private def updateInitialMemberCounters(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = for {
    initialCounters <- store.initialMemberCounters
  } yield {
    countersSupportedAfter.set(initialCounters)
    resolveSequencerPruning(timestamp)
  }

  override def waitForMemberToBeDisabled(member: Member): Future[Unit] =
    memberDisablementPromises.getOrElseUpdate(member, Promise[Unit]()).future

  override def waitForPruningToComplete(timestamp: CantonTimestamp): (Boolean, Future[Unit]) = {
    val newPromise = Promise[Unit]()
    val (isNew, promise) = sequencerPruningPromises
      .putIfAbsent(timestamp, newPromise)
      .fold((true, newPromise))(oldPromise => (false, oldPromise))
    (isNew, promise.future)
  }

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
      priorHead: HeadState,
      member: Member,
  )(implicit traceContext: TraceContext): Future[HeadState] =
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
        val newHead = priorHead
          .focus(_.chunk.ephemeral.status.disabledMembers)
          .modify(_ incl member)
        updateHeadState(priorHead, newHead)
        resolveWaitingForMemberDisablement(member)
        newHead
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

  private def handleChunkUpdate(
      priorHead: HeadState,
      update: ChunkUpdate[SignedChunkEvents],
      dbSequencerIntegration: SequencerIntegration,
  )(implicit
      batchTraceContext: TraceContext
  ): Future[HeadState] = {
    val priorState = priorHead.chunk
    val chunkNumber = priorState.chunkNumber + 1
    val currentBlockNumber = priorHead.block.height + 1
    assert(
      update.newMembers.values.forall(_ >= priorState.lastTs),
      s"newMembers in chunk $chunkNumber of block $currentBlockNumber should be assigned a timestamp after the timestamp of the previous chunk or block",
    )
    assert(
      update.events.view.flatMap(_.timestamps).forall(_ > priorState.lastTs),
      s"Events in chunk $chunkNumber of block $currentBlockNumber have timestamp lower than in the previous chunk or block",
    )
    assert(
      update.lastSequencerEventTimestamp.forall(last =>
        priorState.latestSequencerEventTimestamp.forall(_ < last)
      ),
      s"The last sequencer's event timestamp ${update.lastSequencerEventTimestamp} in chunk $chunkNumber of block $currentBlockNumber  must be later than the previous chunk's or block's latest sequencer event timestamp at ${priorState.latestSequencerEventTimestamp}",
    )

    def checkFirstSequencerCounters: Boolean = {
      val firstSequencerCounterByMember =
        update.events
          .map(_.counters)
          .foldLeft(Map.empty[Member, SequencerCounter])(
            MapsUtil.mergeWith(_, _)((first, _) => first)
          )
      firstSequencerCounterByMember.forall { case (member, firstSequencerCounter) =>
        priorState.ephemeral.headCounter(member).getOrElse(SequencerCounter.Genesis - 1L) ==
          firstSequencerCounter - 1L
      }
    }

    assert(
      checkFirstSequencerCounters,
      s"There is a gap in sequencer counters between the chunk $chunkNumber of block $currentBlockNumber and the previous chunk or block.",
    )

    val lastTs =
      (update.events.view.flatMap(_.timestamps) ++
        update.newMembers.values).maxOption.getOrElse(priorState.lastTs)

    val newState = ChunkState(
      chunkNumber,
      priorState.ephemeral.mergeBlockUpdateEphemeralState(update.state),
      lastTs,
      update.lastSequencerEventTimestamp.orElse(priorState.latestSequencerEventTimestamp),
    )

    if (unifiedSequencer) {
      (for {
        _ <- dbSequencerIntegration.blockSequencerWrites(update.submissionsOutcomes.map(_.outcome))
        _ <- EitherT.right[String](
          dbSequencerIntegration.blockSequencerAcknowledge(update.acknowledgements)
        )

        _ <- EitherT.right[String](
          store.partialBlockUpdate(
            newMembers = Map.empty,
            events = Seq.empty,
            acknowledgments = Map.empty,
            membersDisabled = Seq.empty,
            inFlightAggregationUpdates = update.inFlightAggregationUpdates,
            trafficState = Map.empty,
          )
        )
      } yield {
        val newHead = priorHead.copy(chunk = newState)
        updateHeadState(priorHead, newHead)
        update.acknowledgements.foreach { case (member, timestamp) =>
          resolveAcknowledgements(member, timestamp)
        }
        update.invalidAcknowledgements.foreach { case (member, timestamp, error) =>
          invalidAcknowledgement(member, timestamp, error)
        }
        newHead
      }).valueOr(e =>
        ErrorUtil.internalError(new RuntimeException(s"handleChunkUpdate failed with error: $e"))
      )
    } else {
      // Block sequencer flow
      for {
        _ <- store.partialBlockUpdate(
          newMembers = update.newMembers,
          events = update.events.map(_.events),
          acknowledgments = update.acknowledgements,
          membersDisabled = Seq.empty,
          inFlightAggregationUpdates = update.inFlightAggregationUpdates,
          update.state.trafficState,
        )
        _ <- MonadUtil.sequentialTraverse[(Member, SequencerCounter), Future, Unit](
          update.events
            .flatMap(_.events)
            .collect {
              case (member, tombstone) if tombstone.isTombstone => member -> tombstone.counter
            }
        ) { case (member, counter) => updateMemberCounterSupportedAfter(member, counter) }
      } yield {
        // head state update must happen before member counters are updated
        // as otherwise, if we have a registration in between counter-signalling and head-state,
        // the dispatcher will be initialised with the old head state but not be notified about
        // a change.
        val newHead = priorHead.copy(chunk = newState)
        updateHeadState(priorHead, newHead)
        signalMemberCountersToDispatchers(newState.ephemeral)
        resolveWaitingForMemberDisablement(newState.ephemeral)
        update.acknowledgements.foreach { case (member, timestamp) =>
          resolveAcknowledgements(member, timestamp)
        }
        update.invalidAcknowledgements.foreach { case (member, timestamp, error) =>
          invalidAcknowledgement(member, timestamp, error)
        }
        newHead
      }
    }
  }

  private def handleComplete(priorHead: HeadState, newBlock: BlockInfo)(implicit
      blockTraceContext: TraceContext
  ): Future[HeadState] = {
    val chunkState = priorHead.chunk
    assert(
      chunkState.lastTs <= newBlock.lastTs,
      s"The block's last timestamp must be at least the last timestamp of the last chunk",
    )
    assert(
      chunkState.latestSequencerEventTimestamp <= newBlock.latestSequencerEventTimestamp,
      s"The block's latest topology client timestamp must be at least the last chunk's latest topology client timestamp",
    )

    val newState = BlockEphemeralState(newBlock, chunkState.ephemeral)
    checkInvariantIfEnabled(newState)
    val newHead = HeadState.fullyProcessed(newState)
    for {
      _ <- store.finalizeBlockUpdate(newBlock)
    } yield {
      updateHeadState(priorHead, newHead)
      // Use lastTs here under the following assumptions:
      // 1. lastTs represents the timestamp of the last sequenced "send" event of the last block successfully processed
      //    Specifically, it is the last of the timestamps in the block passed to the rate limiter in the B.U.G for consumed and traffic updates methods.
      //    After setting safeForPruning to this timestamp, we will not be able to request balances from the balance manager prior to this timestamp.
      // 2. This does not impose restrictions on the use of lastSequencerEventTimestamp when calling the rate limiter.
      //    Meaning it should be possible to use an old lastSequencerEventTimestamp when calling the rate limiter, even if it is older than lastTs here.
      //    If this changes, we we will need to use lastSequencerEventTimestamp here instead.
      // 3. TODO(i15837): Under some HA failover scenarios, this may not be sufficient. Mainly because finalizeBlockUpdate above does not
      //    use synchronous commits for DB replicas. This has for consequence that theoretically a block could be finalized but not appear
      //    in the DB replica, while the pruning will be visible in the replica. This would lead the BUG to requesting balances for that block when
      //    reprocessing it, which would fail because the balances have been pruned. This needs to be considered when implementing HA for the BlockSequencer.
      rateLimitManager.safeForPruning(newHead.block.lastTs)
      newHead
    }
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
      newState.headCounter(member).foreach { counter =>
        dispatcher.signalNewHead(counter + 1L)
      }
    }
  }

  private def resolveWaitingForMemberDisablement(newState: EphemeralState): Unit = {
    // if any members that we're waiting to see disabled are now disabled members, complete those promises.
    memberDisablementPromises.keys
      .filter(newState.status.disabledMembers.contains)
      .foreach(resolveWaitingForMemberDisablement)
  }

  private def resolveWaitingForMemberDisablement(disabledMember: Member): Unit =
    memberDisablementPromises.remove(disabledMember) foreach { promise => promise.success(()) }

  private def resolveSequencerPruning(timestamp: CantonTimestamp): Unit = {
    sequencerPruningPromises.remove(timestamp) foreach { promise => promise.success(()) }
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
      error: BaseAlarm,
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
        promise.failure(error.asGrpcError)
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
        headState.get().chunk.ephemeral.headCounter(member) match {
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
        name = show"${sequencerId.uid.identifier.str}-$member",
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
    headState.get().chunk.ephemeral.headCounter(member).foreach { counter =>
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

  /** Arbitrary number of parallelism for signing the sequenced events across chunks of blocks.
    * Within a chunk, the parallelism is unbounded because we use `parTraverse`.
    */
  val chunkSigningParallelism: Int = 10

  def apply(
      protocolVersion: ProtocolVersion,
      domainId: DomainId,
      sequencerId: SequencerId,
      store: SequencerBlockStore,
      enableInvariantCheck: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      rateLimitManager: SequencerRateLimitManager,
      unifiedSequencer: Boolean,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
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
        rateLimitManager = rateLimitManager,
        unifiedSequencer = unifiedSequencer,
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
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )

  object ChunkState {
    val initialChunkCounter = 0L

    def initial(block: BlockEphemeralState): ChunkState =
      ChunkState(
        initialChunkCounter,
        block.state,
        block.latestBlock.lastTs,
        block.latestBlock.latestSequencerEventTimestamp,
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
