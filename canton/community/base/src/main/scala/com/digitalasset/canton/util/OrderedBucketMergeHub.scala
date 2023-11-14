// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import akka.Done
import akka.stream.scaladsl.Source
import akka.stream.stage.{
  AsyncCallback,
  GraphStageLogic,
  GraphStageWithMaterializedValue,
  InHandler,
  OutHandler,
}
import akka.stream.{Attributes, FlowShape, Inlet, KillSwitch, Outlet}
import cats.syntax.functor.*
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext}
import com.digitalasset.canton.util.OrderedBucketMergeHub.OutputElement
import com.digitalasset.canton.util.ShowUtil.*

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/** A custom Akka [[akka.stream.stage.GraphStage]] that merges several ordered source streams into one
  * based on those sources reaching a threshold for equivalent elements.
  *
  * The ordered sources produce elements with totally ordered offsets.
  * For a given threshold `t`, whenever `t` different sources have produced equivalent elements for an offset
  * that is higher than the previous offset, the [[OrderedBucketMergeHub]] emits the map of all these equivalent elements
  * as the next [[com.digitalasset.canton.util.OrderedBucketMergeHub.OutputElement]] to downstream.
  * Elements from the other ordered sources with lower or equal offset that have not yet reached the threshold are dropped.
  *
  * Every correct ordered source should produce the same sequence of offsets.
  * Faulty sources can produce any sequence of elements as they like.
  * The threshold should be set to `F+1` where at most `F` sources are assumed to be faulty,
  * and at least `2F+1` ordered sources should be configured.
  * This ensures that the `F` faulty ordered sources cannot corrupt the stream nor block it.
  *
  * If this assumption is violated, the [[OrderedBucketMergeHub]] may deadlock,
  * as it only looks at the next element of each ordered source
  * (this avoids unbounded buffering and therefore ensures that downstream backpressure reaches the ordered sources).
  * For example, given a threshold of 2 with three ordered sources, two of which are faulty,
  * the first elements of the sources have offsets 1, 2, 3.
  * Suppose that the first ordered source's second element had offset 3 and is equivalent to the third ordered source's first element.
  * Then, by the above definition of merging, the stage could emit the elements with offset 3 and discard those with 1 and 2.
  * However, this is not yet implemented; the stream just does not emit anything.
  * Neither are such deadlocks detected right now.
  * This is because in an asynchronous system, there typically are ordered sources that have not yet delivered their next element,
  * and possibly may never will within useful time, say because they have crashed (which is not considered a fault).
  * In the above example, suppose that the second ordered source had not emitted the element with offset 2.
  * Then it is unknown whether the element with offset 1 should be emitted or not,
  * because we do not know which ordered sources are correct.
  * Suppose we had decided that we drop the elements with offset 1 from a correct ordered source
  * and emit the ones with offset 3 instead,
  * Then the second (delayed, but correct) ordered source can still send an equivalent element with 1,
  * and so the decision of dropping 1 was wrong in hindsight.
  *
  * The [[OrderedBucketMergeHub]] manages the ordered sources.
  * Their configurations and the threshold are coming through the [[OrderedBucketMergeHub]]'s input stream as a [[OrderedBucketMergeConfig]].
  * As soon as a new [[OrderedBucketMergeConfig]] is available,
  * the [[OrderedBucketMergeHub]] changes the ordered sources as necessary:
  *
  * - Ordered sources are identified by their `Name`.
  * - Existing ordered sources whose name does not appear in the new configuration are stopped.
  * - If a new configuration contains a new name for an ordered source, a new ordered source is created using `ops`.
  * - If the configuration of an ordered source changes, the previous source is stopped and a new one with the new configuration is created.
  *
  * The [[OrderedBucketMergeHub]] emits [[com.digitalasset.canton.util.OrderedBucketMergeHub.ControlOutput]] events to downstream:
  *
  * - [[com.digitalasset.canton.util.OrderedBucketMergeHub.NewConfiguration]] signals the new configuration in place.
  * - [[com.digitalasset.canton.util.OrderedBucketMergeHub.ActiveSourceTerminated]] signals
  *   that an ordered source has completed or aborted with an error before it was stopped.
  *
  * Since configuration changes are consumed eagerly, the [[OrderedBucketMergeHub]] buffers
  * these [[com.digitalasset.canton.util.OrderedBucketMergeHub.ControlOutput]] events
  * if downstream is not consuming them fast enough.
  * The stream of configuration changes should therefore be slower than downstream;
  * otherwise, the buffer will grow unboundedly and lead to [[java.lang.OutOfMemoryError]]s eventually.
  *
  * When the configuration stream completes or aborts, all ordered sources are stopped
  * and the output stream completes.
  *
  * An ordered source is stopped by pulling its [[akka.stream.KillSwitch]]
  * and dropping all elements until the source completes or aborts.
  * In particular, the ordered source is not just simply cancelled upon a configuration change
  * or when the configuration stream completes.
  * This allows for properly synchronizing the completion of the [[OrderedBucketMergeHub]] with
  * the internal computations happening in the ordered sources.
  * To that end, the [[OrderedBucketMergeHub]] materializes to a [[scala.concurrent.Future]]
  * that completes when the corresponding futures from all created ordered sources have completed
  * as well as the ordered sources themselves.
  *
  * If downstream cancels, the [[OrderedBucketMergeHub]] cancels all sources and the input port,
  * without draining them. Therefore, the materialized [[scala.concurrent.Future]] may or may not complete,
  * depending on the shape of the ordered sources. For example, if the ordered sources' futures are
  * created with a plain [[akka.stream.scaladsl.FlowOpsMat.watchTermination]], it will complete because
  * [[akka.stream.scaladsl.FlowOpsMat.watchTermination]] completes immediately when it sees a cancellation.
  * Therefore, it is better to avoid downstream cancellations altogether.
  *
  * Rationale for the merging logic:
  *
  * This graph stage is meant to merge the streams of sequenced events from several sequencers on a client node.
  * The operator configures `N` sequencer connections and specifies a threshold `T`.
  * Suppose the operator assumes that at most `F` nodes out of `N` are faulty.
  * So we need `F < T` for safety.
  * For liveness, the operator wants to tolerate as many crashes of correct sequencer nodes as feasible.
  * Let `C` be the number of tolerated crashes.
  * Then `T <= N - C - F` because faulty sequencers may not deliver any messages.
  * For a fixed `F`, `T = F + 1` is optimal as we can then tolerate `C = N - 2F - 1` crashed sequencer nodes.
  *
  * In other words, if the operator wants to tolerate up to `F` faults and up to `C` crashes,
  * then it should set `T = F + 1` and configure `N = 2F + C + 1` different sequencer connections.
  *
  * If more than `C` sequencers have crashed, then the faulty sequencers can make the client deadlock.
  * The client cannot detect this under the asynchrony assumption.
  *
  * Moreover, the client cannot distinguish either between
  * whether a sequencer node is actively malicious or just accidentally faulty.
  * In particular, if several sequencer nodes deliver inequivalent events,
  * we currently silently drop them.
  * TODO(#14365) Design and implement an alert mechanism
  *
  * @param ops The operations for the abstracted-away parameters.
  *            In particular, the equivalence relation between elements is expressed as the pre-image of
  *            the `equals` relation under the [[OrderedBucketMergeHubOps.bucketOf]] function, i.e.,
  *            two elements are equivalent if they end up in the same bucket.
  * @param enableInvariantCheck If true, invariants of the [[OrderedBucketMergeHub]] implementation are checked at run-time.
  *                             Invariant violation are then logged as [[java.lang.IllegalStateException]] and abort the stage with an error.
  *                             Do not enable these checks in production.
  */
class OrderedBucketMergeHub[Name: Pretty, A, Config, Offset: Pretty, M](
    private val ops: OrderedBucketMergeHubOps[Name, A, Config, Offset, M],
    override protected val loggerFactory: NamedLoggerFactory,
    enableInvariantCheck: Boolean,
) extends GraphStageWithMaterializedValue[
      FlowShape[
        OrderedBucketMergeConfig[Name, Config],
        OrderedBucketMergeHub.Output[Name, (Config, Option[M]), A, Offset],
      ],
      Future[Done],
    ]
    with NamedLogging {
  import OrderedBucketMergeHub.*

  type ConfigAndMat = (Config, Option[M])

  private[this] val out: Outlet[Output[Name, ConfigAndMat, A, Offset]] =
    Outlet("OrderedBucketMergeHub.out")
  private[this] val in: Inlet[OrderedBucketMergeConfig[Name, Config]] =
    Inlet("OrderedBucketMergeHub.in")
  override def shape: FlowShape[
    OrderedBucketMergeConfig[Name, Config],
    OrderedBucketMergeHub.Output[Name, ConfigAndMat, A, Offset],
  ] = FlowShape(in, out)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private class BucketingLogic(enclosingAttributes: Attributes) extends GraphStageLogic(shape) {
    // This contains the mutable state of the graph state logic.
    // Since Akka streams runs all the handlers sequentially,
    // we can use plain vars and mutable data structures.
    // We do not need to worry about inter-thread synchronization
    // provided that all accesses are only from within the handlers.

    /** The [[OrderedSource]]s that have been created and not yet fully stopped
      * An [[OrderedSource]] gets added to this map when it is created in [[createActiveSource]].
      * It gets removed when it has completed and all its elements have been emitted or evicted.
      */
    private[this] val orderedSources: mutable.Map[OrderedSourceId, OrderedSource] =
      mutable.Map.empty[OrderedSourceId, OrderedSource]

    /** The data associated with an ordered source and its state.
      *
      * The state evolves according to the following diagram where each box represents one or multiple states.
      * The letters `A`, `B`, and `C` stand for [[OrderedSource.isActive]], [[OrderedSource.isInBucket]],
      * and [[OrderedSource.hasCompleted]], respectively. If they are present in a box,
      * the states represented by the box have the corresponding predicate evaluate to true.
      * If the box prefixes them with `!`, the predicate must evaluate to false in this state.
      * The letter `P` represents whether the [[OrderedSource.inlet]] has been pulled.
      * It is omitted when the pulling state is irrelevant.
      *
      * The `remove` arrows going nowhere indicate that the ordered source ends its lifecycle
      * by being removed from [[orderedSources]].
      *
      * <pre>
      *                                              │start
      *            ┌─────┐         stop           ┌──▼──┐                 ┌─────┐
      *        ┌───┤     ◄────────────────────────┤     │                 │     │
      *        │   │ !A  │                        │  A  │    complete     │  A  │ emit ActiveSourceTermination
      *    next│   │ !B  │                        │ !B  ├─────────────────► !B  ├─────────────────────────────►
      * element│   │ !C  │                        │ !C  │                 │  C  │ remove
      *        │   │  P  │                        │  P  │                 │     │
      *        └───►     ◄────────┐          ┌────►     ◄────────┐        │     │
      *            └──┬──┘        │      emit│    └──┬─┬┘        │        └──▲──┘
      *               │           │    bucket│       │ │         │           │
      *               │           │    if not│       │ └─────────┘           │
      *               │           │  rejected│       │   next element        │
      *               │           │          │       │   with smaller        │
      *               │           │          │       │   offset              │
      *               │           │       ┌──┴──┐    │                       │
      *               │           │       │     │    │                       │
      *               │           │       │  A  │    │next                   │emit bucket
      *               │complete   │       │ !B  │    │element                │if not rejected
      *               │           │       │ !C  │    │with                   │
      *               │           │       │ !P  │    │higher                 │
      *               │           │       │     │    │offset                 │
      *               │           │       └──▲──┘    │                       │
      *               │           │          │       │                       │
      *               │           │          │       │                       │
      *               │           │    remove│       │                       │
      *               │           │    bucket│       │                       │
      *            ┌──▼──┐        │          │    ┌──▼──┐                 ┌──┴──┐        ┌─────┐
      *            │     │        │          └────┤     │                 │     │        │     │
      *            │ !A  │        │               │  A  │                 │  A  │        │ !A  │
      *            │ !B  │        │    stop       │  B  │    complete     │  B  │  stop  │ !B  │
      *    ◄───────┤  C  │        └───────────────┤ !C  ├─────────────────►  C  ├────────►  C  ├───────►
      *     remove │     │                        │ !P  │                 │ !P  │        │ !P  │ remove
      *            │     │                        │     │                 │     │        │     │
      *            └─────┘                        └─────┘                 └─────┘        └─────┘
      * </pre>
      *
      * @param inlet The inlet through which the source's elements are passed.
      *              We pull the inlet immediately upon creation and then whenever the source's last element
      *              is emitted downstream or its bucket is rejected.
      */
    private final class OrderedSource(
        val name: Name,
        val config: Config,
        val inlet: SubSinkInlet[A],
        val killSwitchCell: SingleUseCell[KillSwitch],
    ) {
      import TraceContext.Implicits.Empty.*

      /** Whether the [[OrderedSource]] is active, i.e., it has not yet been stopped
        * due to a configuration change or completion of the configuration stream.
        */
      private var active: Boolean = true
      def isActive: Boolean = active
      def stop(): Unit = {
        if (enableInvariantCheck) {
          ErrorUtil.requireState(isActive, s"Cannot stop the source $name twice.")
        }
        active = false
      }

      /** The bucket, if any, that contains the last element that was pulled from this ordered source. */
      private var lastBucket: Option[ops.Bucket] = None
      def isInBucket: Boolean = lastBucket.nonEmpty
      def addToBucket(bucket: ops.Bucket): Unit = {
        if (enableInvariantCheck) {
          implicit val prettyBucket: Pretty[ops.Bucket] = ops.prettyBucket
          ErrorUtil.requireState(
            lastBucket.isEmpty,
            show"Cannot add source $name to another bucket $bucket. It is already in $lastBucket",
          )
        }
        lastBucket = Some(bucket)
      }
      def removeFromBucket(): Unit = { lastBucket = None }
      def getBucket: Option[ops.Bucket] = lastBucket

      /** [[scala.None$]] as long as the [[com.digitalasset.canton.util.OrderedBucketMergeHub.OrderedSourceSignal.Completed]]
        * signal from the ordered source has not yet been processed.
        * When the [[com.digitalasset.canton.util.OrderedBucketMergeHub.OrderedSourceSignal.Completed]] signal is processed
        * then this is set to contain the completion reason.
        */
      private var completedWith: Option[Option[Throwable]] = None
      def getCompletion: Option[Option[Throwable]] = completedWith
      def hasCompleted: Boolean = completedWith.isDefined
      def completeWith(cause: Option[Throwable]) = {
        if (enableInvariantCheck) {
          ErrorUtil.requireState(
            !hasCompleted,
            show"Cannot complete source $name twice with cause $cause. It has previously been completed with $getCompletion.",
          )
        }
        completedWith = Some(cause)
      }

      def checkInvariant(context: String): Unit = {
        import TraceContext.Implicits.Empty.*
        ErrorUtil.requireState(
          active || !isInBucket,
          s"[$context] Stopped source $name must have an empty lastBucket",
        )
      }
    }

    /** We use our own internal IDs [[com.digitalasset.canton.util.OrderedBucketMergeHub.OrderedSourceId]].
      * `Name`s are not good enough: a reconfiguration may stop the previous ordered source
      * and create a new one with the same name; so we would not be able to distinguish between the old and the new one.
      */
    private[this] val orderedSourceIdGenerator = new AtomicInteger()
    private[this] def nextOrderedSourceId: OrderedSourceId =
      orderedSourceIdGenerator.getAndIncrement()

    /** The currently configured threshold */
    private[this] var currentThreshold: Int = 0

    /** Exclusive lower bound for the next offset to emit. */
    private[this] var lowerBoundNextOffsetExclusive: Offset = ops.exclusiveLowerBoundForBegin

    /** Caches the last value that was queued for emission, if any.
      * If so, its offset is at most [[lowerBoundNextOffsetExclusive]].
      */
    private[this] var lastBucketQueuedForEmission: Option[OutputElement[Name, A]] = None

    /** Contains the equivalence classes of the inspected elements from the ordered sources so far.
      *
      * This is somewhat inefficient: whenever we emit an element,
      * we need to clean up the outdated buckets with lower or equal offset,
      * which looks at all buckets. We could avoid this by keeping the buckets in a map
      * ordered by offset, but this seemed overkill so far given that the expected number of buckets is small
      * (<< 100).
      */
    private[this] val buckets
        : mutable.Map[ops.Bucket, NonEmpty[Seq[BucketElement[OrderedSource, A]]]] =
      mutable.Map.empty[ops.Bucket, NonEmpty[Seq[BucketElement[OrderedSource, A]]]]

    /** Whether upstream has completed and we're now just winding down by stopping all the remaining ordered sources */
    private[this] var upstreamCompleted: Option[Option[Throwable]] = None

    /** A promise for the materialized future.
      * To be completed when the stage has completed and all ordered sources have finished.
      */
    private[this] val completionPromise = Promise[Done]()
    def completionFuture: Future[Done] = completionPromise.future

    /** Collects the completion futures from all ordered sources
      * so that they can be passed into the [[completionFuture]].
      */
    private[this] val flushFutureForOrderedSources =
      new FlushFuture("OrderedBucketMergeHub", loggerFactory)

    // The invariant should hold at the start.
    checkInvariantIfEnabled("Constructor")

    private[this] val outHandler: OutHandler = new OutHandler {
      override def onPull(): Unit = ()

      override def onDownstreamFinish(cause: Throwable): Unit = {
        noTracingLogger.debug("Downstream cancelled: stopping immediately")
        checkInvariantIfEnabled(s"$qualifiedNameOfCurrentFunc begin")
        // Propagate the cancellation upstream
        cancel(in, cause)
        stopAllActiveSources()
        completeStage(None, force = true)
      }
    }
    setHandler(out, outHandler)

    private[this] val configChangeHandler: InHandler = new InHandler {
      override def onPush(): Unit = {
        checkInvariantIfEnabled(s"in-handler $qualifiedNameOfCurrentFunc begin")

        val nextConfig = grab(in)
        noTracingLogger.debug(s"Next config $nextConfig")

        val sourcesToStopB = Seq.newBuilder[(OrderedSourceId, OrderedSource)]
        val namesToKeepB = Set.newBuilder[Name]
        orderedSources.foreach { case (id, orderedSource) =>
          val keep = nextConfig.sources.get(orderedSource.name).contains(orderedSource.config)
          if (keep) namesToKeepB += orderedSource.name
          else if (orderedSource.isActive) sourcesToStopB += (id -> orderedSource)
        }
        val sourcesToStop = sourcesToStopB.result()
        val namesToKeep = namesToKeepB.result()

        val sourcesToCreate = nextConfig.sources.view.filterKeys(!namesToKeep.contains(_))

        sourcesToStop.foreach { case (id, source) => stopActiveSource(id, source) }
        val materializedValues = sourcesToCreate.map { case (name, config) =>
          val materializedValue = createActiveSource(name, config)
          name -> materializedValue
        }.toMap
        val loweredThreshold = currentThreshold > nextConfig.threshold.value
        currentThreshold = nextConfig.threshold.value

        val newConfigAndMat =
          nextConfig.map((name, config) => (config, materializedValues.get(name)))
        emit(out, NewConfiguration(newConfigAndMat, lowerBoundNextOffsetExclusive))

        // Immediately signal demand for the next config change
        pull(in)

        // If the threshold has been lowered, check whether some buckets now reach the threshold.
        // If so, emit the elements in offset order.
        // If several buckets of the same offset reach the threshold, pick one of then non-deterministically.
        // Then clean up the remaining buckets.
        //
        // The non-determinism happens only if the assumption about the number of faulty nodes is violated.
        // In practice, the non-determinism can lead to a ledger fork and is therefore security sensitive.
        //
        // TODO(#14365) Decide what to do if we detect non-determinism
        if (loweredThreshold) {
          implicit val orderingOffset: Ordering[Offset] = ops.orderingOffset
          val fullBucketsByOffset = buckets.toSeq
            .filter { case (_, elems) => elems.sizeIs >= currentThreshold }
            .sortBy { case (bucket, _) => ops.offsetOfBucket(bucket) }
          fullBucketsByOffset.foreach { case (bucket, elems) =>
            emitOrEvictBucket(bucket, elems)
          }
          fullBucketsByOffset.lastOption.foreach { case (bucket, _) =>
            evictBucketsUpToIncluding(ops.offsetOfBucket(bucket))
          }
        }

        checkInvariantIfEnabled(s"in-handler $qualifiedNameOfCurrentFunc end")
      }

      override def onUpstreamFinish(): Unit = {
        noTracingLogger.debug("Config source has completed. Draining all sources...")
        terminateStage(None)
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        noTracingLogger.debug("Config source has aborted. Draining all sources...", ex)
        terminateStage(Some(ex))
      }
    }
    setHandler(in, configChangeHandler)

    private[this] def terminateStage(cause: Option[Throwable]): Unit = {
      checkInvariantIfEnabled(s"$qualifiedNameOfCurrentFunc begin")
      // Do not immediately complete or fail the stage
      // because this would immediately cancel all current ordered sources.
      // Instead, we want to stop and drain them and only afterwards complete or fail the stage.
      upstreamCompleted = Some(cause)
      stopAllActiveSources()
      completeStageIfDone()
      checkInvariantIfEnabled(s"$qualifiedNameOfCurrentFunc end")
    }

    private[this] def stopAllActiveSources(): Unit = {
      orderedSources.foreach { case (id, source) =>
        if (source.isActive) stopActiveSource(id, source)
      }
    }

    override def preStart(): Unit = {
      checkInvariantIfEnabled("preStart")
      // Ask for the first config right at the beginning and make sure that we always pull.
      pull(in)
    }

    override def postStop(): Unit = {
      // Remove references to avoid memory leak
      lastBucketQueuedForEmission = None
    }

    /** Callback used to receive signals from the ordered sources.
      * Avoids that we have to access the mutable states from the ordered source's handlers.
      */
    private[this] val orderedSourceCallback =
      getAsyncCallback[OrderedSourceSignal[Name, A]](processOrderedSourceSignal)

    private[this] def processOrderedSourceSignal(signal: OrderedSourceSignal[Name, A]): Unit =
      signal match {
        case OrderedSourceSignal.NextElement(id, name, elem) =>
          processNextElement(id, name, elem)

        case OrderedSourceSignal.Completed(id, name, cause) =>
          processCompletion(id, name, cause)
      }

    /** Process the next element from the given source, irrespective of whether it is being stopped.
      */
    private[this] def processNextElement(id: OrderedSourceId, name: Name, elem: A): Unit = {
      checkInvariantIfEnabled(
        s"$qualifiedNameOfCurrentFunc begin: id=$id, name=$name, offset: ${ops.offsetOf(elem)}"
      )

      implicit val traceContext: TraceContext = ops.traceContextOf(elem)
      orderedSources.get(id) match {
        case None =>
          // This should only happen if the completion message of a source overtakes the signal for the next element.
          // It is not clear from the specification of getAsyncCallback whether this can happen.
          // But if it happens, it does not really matter; we simply drop the element.
          logger.debug(
            s"Dropping element at offset ${ops.offsetOf(elem)} from inactive source $name (id $id)"
          )
        case Some(source) =>
          // Since we do not keep track of the signals going through the AsyncCallback,
          // we cannot really phrase the following conditions as an invariant check.
          if (enableInvariantCheck) {
            ErrorUtil.requireState(
              source.name == name,
              s"Name of the ordered source ${source.name} (id $id) differed from declared name $name in next-element signal",
            )
            ErrorUtil.requireState(
              !source.isInBucket,
              s"Received a next element from ordered source $name although there is already one waiting in a bucket",
            )
          }

          if (source.isActive) {
            val bucket = ops.bucketOf(elem)
            val offset = ops.offsetOfBucket(bucket)
            if (ops.orderingOffset.compare(offset, lowerBoundNextOffsetExclusive) <= 0) {
              logger.debug(
                show"Dropping next element from source $name with offset $offset because it is not above the lower offset bound of $lowerBoundNextOffsetExclusive"
              )
              pullIfNotCompleted(id)
            } else {
              implicit val prettyBucket: Pretty[ops.Bucket] = ops.prettyBucket
              logger.debug(
                show"Adding element with offset $offset from source $name (id: $id) to bucket $bucket"
              )
              val updatedBucketO = buckets.updateWith(bucket) {
                case None => Some(NonEmpty(Seq, BucketElement(id, source, elem)))
                case Some(elems) => Some(BucketElement(id, source, elem) +: elems)
              }
              val elems = updatedBucketO.getOrElse(
                ErrorUtil.internalError(
                  new IllegalStateException(
                    "updateWith returned None even though the update function always returns Some"
                  )
                )
              )
              source.addToBucket(bucket)

              // Have we have found the next element to emit?
              if (elems.sizeIs >= currentThreshold) {
                emitBucket(bucket, offset, elems)
                evictBucketsUpToIncluding(offset)
              }
            }
          } else {
            // Since the source has been stopped, we do not need to emit a termination signal here,
            // so we can simply pull to drain it.
            logger.debug(
              s"Drained one element at offset ${ops.offsetOf(elem)} from source $name (id $id)"
            )
            pullIgnoringClosed(source.inlet)
          }
      }

      checkInvariantIfEnabled(
        s"$qualifiedNameOfCurrentFunc end: id=$id, name=$name, offset: ${ops.offsetOf(elem)}"
      )
    }

    private[this] def emitBucket(
        bucket: ops.Bucket,
        offset: Offset,
        elems: NonEmpty[Seq[BucketElement[OrderedSource, A]]],
    )(implicit traceContext: TraceContext): Unit = {
      // Unless this is caused by a configuration change,
      // we log this with the trace context that causes the bucket to reach the threshold.
      // This makes sense because in a sequencer client, all the equivalent sequenced events will have the same trace context.
      // So overall we get better tracing than if we used an empty trace context.
      logger.debug(
        s"Bucket $bucket for offset $offset has reached the threshold of $currentThreshold. Emitting next element."
      )
      buckets.remove(bucket).discard[Option[NonEmpty[Seq[BucketElement[OrderedSource, A]]]]]
      removeFromBuckets(elems)
      val merged = elems.map { case BucketElement(_id, source, elem) => source.name -> elem }.toMap
      val output = OutputElement(merged)
      lowerBoundNextOffsetExclusive = offset
      lastBucketQueuedForEmission = Some(output)
      emit(
        out,
        output,
        // Crucially, pull only after the emission.
        // This ensures that the emission buffer for OutputElements remains bounded
        () => pullMultipleIfNotCompleted(elems),
      )
    }

    /** Removes all buckets up to and including `offset` and pulls from the sources whose elements were rejected */
    private[this] def evictBucketsUpToIncluding(offset: Offset): Unit =
      buckets.filterInPlace { (bucket, elems) =>
        val bucketOffset = ops.offsetOfBucket(bucket)
        val retain = ops.orderingOffset.compare(bucketOffset, offset) > 0
        if (!retain) {
          // TODO(#14365) This indicates some faulty nodes. Decide whether we should alert
          removeBucketElements(bucket, bucketOffset, elems)
        }
        retain
      }

    /** Emit (and pull thereafter) or evict (and pull immediately) the given bucket,
      * depending on whether its offset is above the [[lowerBoundNextOffsetExclusive]].
      */
    private[this] def emitOrEvictBucket(
        bucket: ops.Bucket,
        elems: NonEmpty[Seq[BucketElement[OrderedSource, A]]],
    ): Unit = {
      import TraceContext.Implicits.Empty.*
      val offset = ops.offsetOfBucket(bucket)
      if (ops.orderingOffset.compare(offset, lowerBoundNextOffsetExclusive) > 0) {
        emitBucket(bucket, offset, elems)
      } else {
        buckets.remove(bucket).discard[Option[NonEmpty[Seq[BucketElement[OrderedSource, A]]]]]
        removeBucketElements(bucket, offset, elems)
      }
    }

    /** Updates the ordered sources after their elements have been rejected from the bucket and pulls them */
    private[this] def removeBucketElements(
        bucket: ops.Bucket,
        offset: Offset,
        elems: NonEmpty[Seq[BucketElement[OrderedSource, A]]],
    ): Unit = {
      if (logger.underlying.isDebugEnabled) {
        implicit val prettyBucket: Pretty[ops.Bucket] = ops.prettyBucket
        val droppedSources = elems.map { case BucketElement(id, source, elem) =>
          s"${source.name} (id: $id)".unquoted
        }
        noTracingLogger.debug(
          show"Dropping bucket $bucket for offset $offset with ${elems.size} sources: $droppedSources"
        )
      }
      removeFromBuckets(elems)
      pullMultipleIfNotCompleted(elems)
    }

    /** Process the signal that an ordered source has completed.
      * If the ordered source still has a last element in a bucket,
      * we wait until its fate is decided before we output the termination signal.
      */
    private[this] def processCompletion(
        id: OrderedSourceId,
        name: Name,
        cause: Option[Throwable],
    ): Unit = {
      checkInvariantIfEnabled(s"$qualifiedNameOfCurrentFunc begin: $name (id $id)")
      orderedSources.get(id) match {
        case None =>
          noTracingLogger.error(
            s"Processed completion signal from inactive source $name (id $id)"
          )
          completeStageIfDone()
        case Some(source) =>
          noTracingLogger.debug(s"Processing completion signal from source $name (id $id)")
          source.completeWith(cause)
          if (!source.isInBucket) {
            orderedSources.remove(id).discard[Option[OrderedSource]]
            if (source.isActive) {
              // It is safe to call emit here because if the source's last element is still waiting for emission,
              // then this emission will be queued afterwards and thus come after the source's last element.
              emit(
                out,
                ActiveSourceTerminated(source.name, cause),
                () => completeStageIfDone(),
              )
            } else {
              // No need to emit a termination signal as we're draining the source
              completeStageIfDone()
            }
          } else {
            // Nothing to do here.
            // The source is active by the invariant because lastBucket is non-empty.
            // The source's lastBucket element will eventually be emitted or rejected,
            // and then we will call `pullIfNotCompleted`.
            // At that point, we will emit the termination event
            // unless the source has been stopped in between.
          }
      }
      checkInvariantIfEnabled(s"$qualifiedNameOfCurrentFunc end: $name (id $id)")
    }

    private[this] def completeStageIfDone(): Unit =
      upstreamCompleted.foreach { cause => completeStage(cause, force = false) }

    private[this] def completeStage(cause: Option[Throwable], force: Boolean): Unit = {
      lazy val outstanding = orderedSources.map { case (id, source) =>
        s"${source.name} (id $id)".unquoted
      }.toSeq
      if (orderedSources.isEmpty || force) {
        noTracingLogger.debug("Completing the OrderedBucketMergeHub stage")
        if (orderedSources.nonEmpty) {
          noTracingLogger.info(
            show"Forcefully cancelling the remaining ordered sources: $outstanding"
          )
          orderedSources.foreach { case (_id, source) => source.inlet.cancel() }
        }
        cause.fold(completeStage())(failStage)
        val directExecutionContext = DirectExecutionContext(noTracingLogger)
        completionPromise.completeWith(
          flushFutureForOrderedSources.flush().map(_ => Done)(directExecutionContext)
        )
      } else
        noTracingLogger.debug(
          show"Cannot complete OrderedBucketMergeHub stage due to remaining ordered sources: $outstanding"
        )
    }

    private[this] def removeFromBuckets(elems: Seq[BucketElement[OrderedSource, A]]): Unit =
      elems.foreach { case BucketElement(_id, source, _elem) => source.removeFromBucket() }

    private[this] def pullMultipleIfNotCompleted(
        toPull: Seq[BucketElement[OrderedSource, A]]
    ): Unit =
      toPull.foreach { case BucketElement(id, _, _) => pullIfNotCompleted(id) }

    private[this] def pullIfNotCompleted(id: OrderedSourceId): Unit = {
      orderedSources.get(id).foreach { source =>
        source.getCompletion match {
          case None => pullIgnoringClosed(source.inlet)
          case Some(cause) =>
            noTracingLogger.debug(s"Removing completed source ${source.name} (id $id)")
            // The ordered source has completed beforehand. Let's send the termination signal!
            orderedSources.remove(id).discard[Option[OrderedSource]]
            emit(out, ActiveSourceTerminated(source.name, cause), () => completeStageIfDone())
        }
      }
    }

    private[this] def pullIgnoringClosed(inlet: SubSinkInlet[A]): Unit = {
      // Use exception handling instead of checking .isClosed
      // because it is unclear whether closing can happen concurrently.
      try {
        inlet.pull()
      } catch {
        case e: IllegalArgumentException if e.getMessage.contains("cannot pull closed port") =>
        // If the ordered source has already been closed, then there has been or will be a Completed callback
        // that produces the ActiveSourceTerminated output if necessary and remove the source
      }
    }

    private[this] def createActiveSource(name: Name, config: Config): M = {
      val id = nextOrderedSourceId
      val newSource = ops.makeSource(
        name,
        config,
        lowerBoundNextOffsetExclusive,
        lastBucketQueuedForEmission.map(ops.toPriorElement).orElse(ops.priorElement),
      )
      val subsink = new SubSinkInlet[A](s"OrderedMergeHub.sink($name-$id)")
      subsink.setHandler(
        new ActiveSourceInHandler(id, name, () => subsink.grab(), orderedSourceCallback)
      )

      val killSwitchCell = new SingleUseCell[KillSwitch]
      val source = new OrderedSource(name, config, subsink, killSwitchCell)
      orderedSources.put(id, source).discard[Option[OrderedSource]]

      val graph = newSource.to(subsink.sink)
      val (killSwitch, doneF, materializedValue) = subFusingMaterializer.materialize(
        graph,
        defaultAttributes = enclosingAttributes,
      )

      killSwitchCell.putIfAbsent(killSwitch).discard[Option[KillSwitch]]
      flushFutureForOrderedSources.addToFlushWithoutLogging(s"source $name (id $id)")(doneF)

      subsink.pull()
      materializedValue
    }

    private[this] def stopActiveSource(id: OrderedSourceId, source: OrderedSource): Unit = {
      val killSwitch = source.killSwitchCell.getOrElse {
        import TraceContext.Implicits.Empty.*
        ErrorUtil.invalidState(s"Kill switch for source ${source.name} (id: $id) is not available")
      }
      noTracingLogger.debug(s"Stopping source ${source.name} (id $id) by pulling its kill switch")
      killSwitch.shutdown()
      source.stop()

      // Remove the source's elements from its bucket.
      source.getBucket match {
        case Some(bucket) =>
          noTracingLogger.debug(
            s"Removing ${source.name} (id $id)'s last element from bucket $bucket"
          )
          buckets
            .updateWith(bucket) {
              case Some(elems) => NonEmpty.from(elems.filterNot(_.id == id))
              case None =>
                import TraceContext.Implicits.Empty.*
                ErrorUtil.invalidState(
                  s"Invariant violation: Ordered source ${source.name} (id $id) is not present in its lastBucket"
                )
            }
            .discard[Option[NonEmpty[Seq[BucketElement[OrderedSource, A]]]]]
          source.removeFromBucket()
          if (!source.hasCompleted) {
            pullIgnoringClosed(source.inlet)
          } else {
            orderedSources.remove(id).discard[Option[OrderedSource]]
          }
        case None =>
        // Nothing to do here
        // If the source has been pulled, then it will eventually send a NextElement or Completion signal
        // and this signal will either pull again or remove the source as a whole.
        // Otherwise, an element of the source is in the emission buffer,
        // so we will pull the source again after it has been emitted.
      }
    }

    private[this] def checkInvariantIfEnabled(context: => String): Unit =
      if (enableInvariantCheck) invariant(context) else ()

    private[this] def invariant(context: String): Unit = {
      import TraceContext.Implicits.Empty.*
      implicit val prettyBucket: Pretty[ops.Bucket] = ops.prettyBucket
      // Must only be called from the main graph stage logic and not asynchronously!

      def sourcesInBucketsConsistent(): Unit = {
        buckets.foreach { case (bucket, elems) =>
          elems.foreach { elem =>
            val source = orderedSources.getOrElse(
              elem.id,
              ErrorUtil.internalError(
                new IllegalStateException(
                  s"[$context] Bucket $bucket refers to nonexistent source ${elem.source.name} (id: ${elem.id})"
                )
              ),
            )
            ErrorUtil.requireState(
              source eq elem.source,
              s"[$context] Bucket $bucket's source id ${elem.id} refers to a different source than orderedSources: ${elem.source} vs. $source",
            )
          }
        }
      }

      def uniqueBucketedSources(): Unit = {
        val namesInMoreThanOneBucket =
          buckets.toSeq
            .flatMap { case (bucket, elems) => elems.map(_.source.name -> bucket) }
            .groupBy(_._1)
            .filter { case (_, buckets) => buckets.sizeIs > 1 }
        ErrorUtil.requireState(
          namesInMoreThanOneBucket.isEmpty,
          show"[$context] Source names appear multiple times in the buckets: $namesInMoreThanOneBucket",
        )
        // Since we check that orderedSources is consistent with buckets, we can deduce that each ID appears at most once.
      }

      def lastBucketExists(): Unit = {
        val lastBuckets = orderedSources.flatMap { case (id, source) =>
          source.getBucket.map(_ -> id -> source.name).toList
        }
        lastBuckets.foreach { case ((bucket, id), name) =>
          val elems = buckets.getOrElse(
            bucket,
            ErrorUtil.internalError(
              new IllegalStateException(
                s"[$context] Source $name (id $id)'s lastBucket refers to non-existent bucket $bucket"
              )
            ),
          )
          ErrorUtil.requireState(
            elems.exists(_.id == id),
            s"[$context] Source $id's lastBucket $bucket does not contain an element from the source",
          )
        }
      }

      def lastBucketComplete(): Unit = {
        buckets.foreach { case (bucket, elems) =>
          elems.foreach { elem =>
            val lastBucket = elem.source.getBucket
            ErrorUtil.requireState(
              lastBucket.contains(bucket),
              s"[$context] Source ${elem.source.name} (id: ${elem.id}) does not contain the bucket its element is in: $lastBucket",
            )
          }
        }
      }

      def bucketsBelowThreshold(): Unit = {
        buckets.foreach { case (bucket, elems) =>
          ErrorUtil.requireState(
            elems.sizeIs < currentThreshold,
            s"[$context] Bucket $bucket has more (${elems.size}) elements than the current threshold $currentThreshold",
          )
        }
      }

      def orderedSourceInvariant(): Unit = {
        orderedSources.foreach { case (id, source) =>
          source.checkInvariant(s"$context/source ${source.name} (id $id)")
        }
      }

      def lastBucketQueuedForEmissionInvariant(): Unit = {
        lastBucketQueuedForEmission.foreach { case OutputElement(elems) =>
          val buckets = elems.values.map(ops.bucketOf).toSeq
          ErrorUtil.requireState(
            buckets.distinct.sizeIs == 1,
            s"[$context] Last bucket queued for emission contains elements from different buckets: $buckets",
          )

          val (_, elem) = elems.head1
          val offset = ops.offsetOf(elem)
          ErrorUtil.requireState(
            ops.orderingOffset.compare(offset, lowerBoundNextOffsetExclusive) <= 0,
            s"[$context] Last bucket queued for emission with offset $offset must at most be the lower bound at $lowerBoundNextOffsetExclusive",
          )
        }
      }

      sourcesInBucketsConsistent()
      uniqueBucketedSources()
      lastBucketExists()
      lastBucketComplete()
      bucketsBelowThreshold()
      orderedSourceInvariant()
      lastBucketQueuedForEmissionInvariant()
    }
  }

  /** This handler receives the elements from an ordered source.
    * It belongs to a different materialized graph than the [[BucketingLogic]],
    * so it must not access its mutable state. To enforce this,
    * this class is lexicographically outside of the [[BucketingLogic]].
    * Instead, we go through the provided [[akka.stream.stage.AsyncCallback]]
    * to signal the arrival and completion thread-safely.
    */
  private[this] class ActiveSourceInHandler(
      id: OrderedSourceId,
      name: Name,
      grab: () => A,
      callback: AsyncCallback[OrderedSourceSignal[Name, A]],
  ) extends InHandler {
    override def onPush(): Unit = {
      val elem = grab()
      if (logger.underlying.isDebugEnabled) {
        implicit val traceContext: TraceContext = ops.traceContextOf(elem)
        logger.debug(
          s"Signalling element with offset ${ops.offsetOf(elem)} from source $name (id $id)"
        )
      }
      callback.invoke(
        OrderedBucketMergeHub.OrderedSourceSignal.NextElement(id, name, elem)
      )
    }

    override def onUpstreamFinish(): Unit = {
      noTracingLogger.debug(s"Signalling completion of source $name (id $id)")
      callback.invoke(
        OrderedBucketMergeHub.OrderedSourceSignal.Completed(id, name, None)
      )
    }

    override def onUpstreamFailure(ex: Throwable): Unit = {
      noTracingLogger.debug(s"Signalling abortion of source $name (id $id)", ex)
      callback.invoke(
        OrderedBucketMergeHub.OrderedSourceSignal.Completed(id, name, Some(ex))
      )
    }
  }

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Done]) = {
    val logic = new BucketingLogic(inheritedAttributes)
    logic -> logic.completionFuture
  }
}

object OrderedBucketMergeHub {

  /** Outputs of the [[OrderedBucketMergeHub]], combines actual data with control messages */
  sealed trait Output[Name, +ConfigAndMat, +A, +Offset] extends Product with Serializable {
    def map[ConfigAndMat2, A2, Offset2](
        fConfigAndMat: (Name, ConfigAndMat) => ConfigAndMat2,
        fA: (Name, A) => A2,
        fOffset: Offset => Offset2,
    ): Output[Name, ConfigAndMat2, A2, Offset2]
  }

  /** Actual data output */
  final case class OutputElement[Name, +A](elem: NonEmpty[Map[Name, A]])
      extends Output[Name, Nothing, A, Nothing] {
    def map[A2](fA: (Name, A) => A2): OutputElement[Name, A2] = OutputElement(
      elem.map { case (name, a) => name -> fA(name, a) }.toMap
    )
    override def map[ConfigAndMat2, A2, Offset2](
        fConfigAndMat: (Name, Nothing) => ConfigAndMat2,
        fA: (Name, A) => A2,
        fOffset: Nothing => Offset2,
    ): OutputElement[Name, A2] = map(fA)
  }

  sealed trait ControlOutput[Name, +ConfigAndMat, +Offset]
      extends Output[Name, ConfigAndMat, Nothing, Offset] {
    def map[Config2, Offset2](
        fConfigAndMat: (Name, ConfigAndMat) => Config2,
        fOffset: Offset => Offset2,
    ): ControlOutput[Name, Config2, Offset2]

    override def map[Config2, A2, Offset2](
        fConfigAndMat: (Name, ConfigAndMat) => Config2,
        fA: (Name, Nothing) => A2,
        fOffset: Offset => Offset2,
    ): ControlOutput[Name, Config2, Offset2] = map(fConfigAndMat, fOffset)
  }

  /** Signals the new configuration that is active for all subsequent elements until the next [[NewConfiguration]]
    * and the materialized values for the newly created sources.
    */
  final case class NewConfiguration[Name, +ConfigAndMat, +Offset](
      newConfig: OrderedBucketMergeConfig[Name, ConfigAndMat],
      startingOffset: Offset,
  ) extends ControlOutput[Name, ConfigAndMat, Offset] {
    override def map[ConfigAndMat2, Offset2](
        fConfigAndMat: (Name, ConfigAndMat) => ConfigAndMat2,
        fOffset: Offset => Offset2,
    ): NewConfiguration[Name, ConfigAndMat2, Offset2] = NewConfiguration(
      newConfig.map(fConfigAndMat),
      fOffset(startingOffset),
    )
  }

  /** Signals that the source has terminated with the given cause.
    * Downstream is responsible for reacting to the termination signal
    * and changing the configuration if necessary.
    */
  final case class ActiveSourceTerminated[Name](name: Name, cause: Option[Throwable])
      extends ControlOutput[Name, Nothing, Nothing] {
    override def map[Config2, Offset2](
        fConfigAndMat: (Name, Nothing) => Config2,
        fOffset: Nothing => Offset2,
    ): ActiveSourceTerminated[Name] = this
  }

  /** The internal type of IDs for ordered sources */
  private type OrderedSourceId = Int

  /** Internal signal between the ordered sources and the graph stage logic of the [[OrderedBucketMergeHub]] */
  private sealed trait OrderedSourceSignal[Name, +A] extends Product with Serializable {
    def id: OrderedSourceId

    def name: Name
  }

  private object OrderedSourceSignal {
    final case class NextElement[Name, +A](
        override val id: OrderedSourceId,
        override val name: Name,
        elem: A,
    ) extends OrderedSourceSignal[Name, A]

    final case class Completed[Name](
        override val id: OrderedSourceId,
        override val name: Name,
        cause: Option[Throwable],
    ) extends OrderedSourceSignal[Name, Nothing]
  }

  private final case class BucketElement[S, +A](
      id: OrderedSourceId,
      source: S,
      elem: A,
  )
}

/** @param threshold The threshold of equivalent elements to reach before it can be emitted.
  * @param sources The configurations to be used with [[OrderedBucketMergeHubOps.makeSource]] to create a source.
  */
final case class OrderedBucketMergeConfig[Name, +Config](
    threshold: PositiveInt,
    sources: NonEmpty[Map[Name, Config]],
) {
  def map[Config2](f: (Name, Config) => Config2): OrderedBucketMergeConfig[Name, Config2] =
    OrderedBucketMergeConfig(
      threshold,
      sources.map { case (name, config) => name -> f(name, config) }.toMap,
    )
}

trait OrderedBucketMergeHubOps[Name, A, Config, Offset, +M] {

  /** The type of equivalence classes for the merged elements */
  type Bucket
  def prettyBucket: Pretty[Bucket]

  /** Defines an equivalence relation on `A` */
  def bucketOf(x: A): Bucket

  /** The ordering for the offsets.
    * This defines a total preorder (AKA total quasi-order) on buckets
    * and elements via the projections [[offsetOfBucket]] and [[bucketOf]]
    */
  def orderingOffset: Ordering[Offset]

  def offsetOfBucket(bucket: Bucket): Offset

  // Make sure that the offset assignment respects bucketing
  final def offsetOf(x: A): Offset = offsetOfBucket(bucketOf(x))

  /** The initial offset to start from */
  def exclusiveLowerBoundForBegin: Offset

  /** The type of prior elements that is passed to [[makeSource]].
    * [[toPriorElement]] defines an abstraction function from
    * [[com.digitalasset.canton.util.OrderedBucketMergeHub.OutputElement]]s.
    */
  type PriorElement

  /** The prior element to be passed to [[makeSource]] at the start */
  def priorElement: Option[PriorElement]

  /** An abstraction function from [[com.digitalasset.canton.util.OrderedBucketMergeHub.OutputElement]] to [[PriorElement]]
    */
  def toPriorElement(output: OutputElement[Name, A]): PriorElement

  def traceContextOf(x: A): TraceContext

  /** Creates a new source upon a config change.
    * The returned source is materialized at most once.
    * To close the source, the materialized [[akka.stream.KillSwitch]] is pulled
    * and the source is drained until it completes.
    * The materialized [[scala.concurrent.Future]] should complete when all internal computations have stopped.
    * The [[OrderedBucketMergeHub]]'s materialized [[scala.concurrent.Future]] completes only after
    * these materialized futures of all created ordered sources have completed.
    *
    * @param priorElement The prior element that last reached the threshold or [[priorElement]] if there was none.
    */
  def makeSource(
      name: Name,
      config: Config,
      exclusiveStart: Offset,
      priorElement: Option[PriorElement],
  ): Source[A, (KillSwitch, Future[Done], M)]
}

object OrderedBucketMergeHubOps {
  def apply[Name, A <: HasTraceContext, Config, Offset: Ordering, B: Pretty, M](
      initialOffset: Offset
  )(toBucket: A => B, toOffset: B => Offset)(
      mkSource: (Name, Config, Offset, Option[A]) => Source[A, (KillSwitch, Future[Done], M)]
  ): OrderedBucketMergeHubOps[Name, A, Config, Offset, M] =
    new OrderedBucketMergeHubOps[Name, A, Config, Offset, M] {
      override type PriorElement = A
      override type Bucket = B
      override def prettyBucket: Pretty[Bucket] = implicitly
      override def bucketOf(x: A): Bucket = toBucket(x)
      override def orderingOffset: Ordering[Offset] = implicitly
      override def offsetOfBucket(bucket: Bucket): Offset = toOffset(bucket)
      override def exclusiveLowerBoundForBegin: Offset = initialOffset
      override def traceContextOf(x: A): TraceContext = x.traceContext
      override def makeSource(
          name: Name,
          config: Config,
          exclusiveStart: Offset,
          priorElement: Option[PriorElement],
      ): Source[A, (KillSwitch, Future[Done], M)] =
        mkSource(name, config, exclusiveStart, priorElement)
      override def priorElement: Option[A] = None
      override def toPriorElement(output: OutputElement[Name, A]): A = output.elem.head1._2
    }
}
