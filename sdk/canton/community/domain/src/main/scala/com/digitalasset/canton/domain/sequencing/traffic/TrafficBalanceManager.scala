// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.flatMap.*
import com.digitalasset.canton.caching.CaffeineCache
import com.digitalasset.canton.caching.CaffeineCache.FutureAsyncCacheLoader
import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.domain.sequencing.traffic.TrafficBalanceManager.*
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficBalanceStore
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}
import com.github.benmanes.caffeine.cache as caffeine

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.compat.java8.FutureConverters.*
import scala.concurrent.{ExecutionContext, Future, blocking}

/** Manages traffic balances for sequencer members.
  * This borrows concepts from topology management, in a simplified way.
  * There is no "change delay", which has the direct consequence that traffic balance updates need to be processed sequentially
  * before subsequent messages read in the BlockUpdateGenerator can be processed.
  * We also only here keep track of the latest *few* balance updates for each member in memory.
  * Older balance states will have to be fetched from the database.
  * Balances can be automatically pruned from the cache AND the store according to pruningRetentionWindow.
  */
class TrafficBalanceManager(
    val store: TrafficBalanceStore,
    clock: Clock,
    trafficConfig: SequencerTrafficConfig,
    futureSupervisor: FutureSupervisor,
    sequencerMetrics: SequencerMetrics,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {
  private val lastUpdateAt: AtomicReference[Option[CantonTimestamp]] =
    new AtomicReference[Option[CantonTimestamp]](None)
  private val pendingBalanceUpdates = new mutable.PriorityQueue[PendingBalanceUpdate]()(
    // Reverse because higher timestamp means "younger", and we want to dequeue from oldest to youngest
    Ordering.by[PendingBalanceUpdate, CantonTimestamp](_.lastSeen).reverse
  )
  // Holds a timestamp, signalled externally, before which it is safe to prune traffic balances.
  // If empty, we cannot prune.
  private val safeToPruneBeforeExclusive = new AtomicReference[Option[CantonTimestamp]](None)

  // Holds the result of the current auto pruning processing, if any
  private val autoPruningPromise = new AtomicReference[Option[PromiseUnlessShutdown[Unit]]](None)

  // Async caffeine cache holding the last few balance updates for each member
  private val trafficBalances
      : CaffeineCache.AsyncLoadingCaffeineCache[Member, BalancesForMember] = {
    import TraceContext.Implicits.Empty.emptyTraceContext
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        // Automatically cleans up inactive members from the cache
        .expireAfterAccess(trafficConfig.pruningRetentionWindow.asJava)
        .maximumSize(trafficConfig.maximumTrafficBalanceCacheSize.value.toLong)
        .buildAsync(
          new FutureAsyncCacheLoader[Member, BalancesForMember](
            store
              .lookup(_)
              .map(balances =>
                BalancesForMember(SortedMap(balances.map(b => b.sequencingTimestamp -> b)*))
              )
          )
        ),
      sequencerMetrics.trafficControl.balanceCache,
    )
  }

  lazy val subscription = new SequencerTrafficControlSubscriber(this, loggerFactory)

  /** Initializes the traffic manager lastUpdateAt with the initial timestamp from the store if it's not already set.
    * Call before using the manager.
    */
  def initialize(implicit tc: TraceContext) = {
    store.getInitialTimestamp.map {
      case Some(initialTs) =>
        logger.debug(s"Initializing manager with $initialTs from store")
        if (lastUpdateAt.compareAndSet(None, Some(initialTs)))
          logger.debug(
            s"Traffic balance manager 'lastUpdateAt' initialized with $initialTs from store"
          )
        else
          ErrorUtil.invalidState("Manager was initialized more than once.")
      case _ =>
        logger.debug("No initial timestamp found in traffic balance store")
    }
  }

  /** Timestamp of the last update made to the manager
    */
  def maxTsO = lastUpdateAt.get()

  /** Notify this class that the provided timestamp has been observed by the sequencer client.
    * Together with [[addTrafficBalance]], this method is expected to be called sequentially with increasing timestamps.
    */
  def tick(
      timestamp: CantonTimestamp
  )(implicit tc: TraceContext): Unit = {
    updateAndCompletePendingUpdates(timestamp)
  }

  /** Add a new traffic balance to the store and the cache.
    * Balances with a serial less or equal to the most recent one will be ignored.
    * Together with [[tick]], this method expects to be called sequentially with balances in increasing order of sequencingTimestamp.
    * This method MUST NOT be called concurrently.
    */
  def addTrafficBalance(
      balance: TrafficBalance
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.debug(s"Updating traffic balance: $balance")
    trafficBalances.underlying
      .asMap()
      .compute(
        balance.member,
        (_, existingBalancesO) =>
          Option(existingBalancesO) match {
            case Some(existingBalancesF) =>
              existingBalancesF.toScala
                .map {
                  case balanceForMember @ BalancesForMember(existingBalances, _)
                      if existingBalances.values.maxOption.forall(_.serial < balance.serial) =>
                    logger.trace(s"Updating with new balance: $balance")
                    balanceForMember.copy(
                      trafficBalances = existingBalances
                        // Limit how many balances we keep in memory
                        // trafficBalanceCacheSizePerMember is a PositiveInt so this will be 0 at the lowest
                        .takeRight(
                          checked(trafficConfig.trafficBalanceCacheSizePerMember.value - 1)
                        ) + (balance.sequencingTimestamp -> balance)
                    )
                  case balanceForMember @ BalancesForMember(existingBalances, _) =>
                    logger.debug(
                      s"Ignoring outdated traffic balance update: $balance, existing balances are ${existingBalances.values}"
                    )
                    balanceForMember
                }
                .toJava
                .toCompletableFuture
            case _ =>
              Future(
                BalancesForMember(SortedMap(balance.sequencingTimestamp -> balance))
              ).toJava.toCompletableFuture
          },
      )
      .toScala
      .flatMap {
        // Only insert in the store if the last balance in the cache is indeed the new one - this allows to reuse whatever
        // checks the cache logic above performed
        case balances if balances.trafficBalances.lastOption.forall({ case (_, cachedBalance) =>
              cachedBalance == balance
            }) =>
          store
            .store(balance)
            .map(_ => balances)
        case balances => Future.successful(balances)
      }
      // Increment the metric
      .flatTap(_ => Future.successful(sequencerMetrics.trafficControl.balanceUpdateProcessed.inc()))
      .map { balances =>
        // Schedule the pruning asynchronously to not slow down processing
        // However, this means that if the pruning retention window is very short and we get a lot of updates, we may
        // end up piling up these futures.
        FutureUtil.doNotAwait(
          pruneMemberIfEligible(balance.member, balances),
          s"Failed to prune traffic balances for member ${balance.member}",
        )
        updateAndCompletePendingUpdates(balance.sequencingTimestamp)
      }
  }

  private def pruneMemberIfEligible(member: Member, balances: BalancesForMember)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    balances match {
      case BalancesForMember(_, eligibleForPruningBefore) =>
        eligibleForPruningBefore.getAndSet(None) match {
          case Some(pruningTimestamp) =>
            logger.debug(s"Pruning traffic balances for $member below $eligibleForPruningBefore")
            store
              .pruneBelowExclusive(member, pruningTimestamp)
              .map { _ =>
                logger.trace(
                  s"Traffic balances for $member were pruned below $eligibleForPruningBefore"
                )
              }
          case _ => Future.successful(())
        }
      case _ => Future.successful(())
    }
  }

  /** Get the balance valid at the given timestamp from the provided sorted map
    */
  private def balanceValidAt(
      balances: SortedMap[CantonTimestamp, TrafficBalance],
      timestamp: CantonTimestamp,
  ): Option[TrafficBalance] = {
    // maxBefore is exclusive with the upper bound, therefore we consider balances effective
    // at the timestamp immediately following the sequencing timestamp
    balances.maxBefore(timestamp).map(_._2)
  }

  /** Get the balance valid at the given timestamp from the provided seq
    */
  private def balanceValidAt(
      balances: Seq[TrafficBalance],
      timestamp: CantonTimestamp,
  ): Option[TrafficBalance] = {
    balanceValidAt(SortedMap.from(balances.map(b => b.sequencingTimestamp -> b)), timestamp)
  }

  /** Return the balance at a given timestamp, going to the DB if necessary.
    * If the balance cannot be found in the DB despite having at least one balance persisted,
    * return an error, as it means the balance at that timestamp must have been pruned.
    */
  private def getBalanceAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficBalanceManagerError, Option[TrafficBalance]] = {
    EitherT
      .liftF[Future, TrafficBalanceManagerError, BalancesForMember](trafficBalances.get(member))
      .flatMap {
        // We've got *some* balance in the cache
        case BalancesForMember(balances, _) =>
          // See if we have the requested timestamp
          balanceValidAt(balances, timestamp) match {
            case Some(value) =>
              EitherT.rightT[Future, TrafficBalanceManagerError](Option(value))
            // If not, load balances from the DB
            // Note that the caffeine cache would have loaded the balances if the cache was empty for that member.
            // But because we don't keep all balances in the cache, if the cacne is not empty, we won't reload from the DB,
            // and we might not have all the values in the cache. So to be sure, we explicitly get all balances from the DB.
            // If this happen too frequently, one should consider increasing the trafficBalanceCacheSizePerMember config value.
            case _ =>
              sequencerMetrics.trafficControl.balanceCacheMissesForTimestamp.inc()
              EitherT
                .liftF[Future, TrafficBalanceManagerError, Seq[TrafficBalance]](
                  store.lookup(member)
                )
                .map(balanceValidAt(_, timestamp))
                .flatMap {
                  // If we can't find the balance in the DB, and the timestamp we're asking is older than safeToPruneBeforeExclusive,
                  // return an error, because the balance may have been pruned.
                  case None if safeToPruneBeforeExclusive.get().exists(timestamp < _) =>
                    EitherT
                      .leftT[Future, Option[TrafficBalance]](
                        TrafficBalanceAlreadyPruned(member, timestamp)
                      )
                      .leftWiden[TrafficBalanceManagerError]
                  // Otherwise return the balance as is
                  case balance =>
                    EitherT.rightT[Future, TrafficBalanceManagerError](balance)

                }
          }
      }
      .mapK(FutureUnlessShutdown.outcomeK)
  }

  /** Update [[lastUpdatedAt]], and complete pending updates for which we now have the valid balance.
    * @param timestamp timestamp of the last update
    */
  private def updateAndCompletePendingUpdates(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Unit = {
    logger.trace(s"Updating lastUpdatedAt to $timestamp")
    val prev = lastUpdateAt.getAndSet(Some(timestamp))

    prev match {
      case previous if previous.forall(_ <= timestamp) =>
        blocking {
          pendingBalanceUpdates.synchronized {
            logger.trace(s"Dequeueing pending balances up until $timestamp")
            dequeueUntil(timestamp).foreach { update =>
              logger.trace(s"Providing balance update at timestamp ${update.desired}")
              update.promise.completeWith(getBalanceAt(update.member, update.desired).value)
            }
          }
        }
      case _ =>
        ErrorUtil.invalidState(
          s"Received an update out of order: Update = $timestamp. Previous timestamp was $prev"
        )
    }
  }

  private def dequeueUntil(timestamp: CantonTimestamp): List[PendingBalanceUpdate] = {
    @tailrec
    def go(acc: List[PendingBalanceUpdate]): List[PendingBalanceUpdate] = {
      if (pendingBalanceUpdates.headOption.exists(_.lastSeen <= timestamp))
        go(pendingBalanceUpdates.dequeue() +: acc)
      else
        acc
    }
    go(List.empty)
  }

  /** Return the latest known balance for the given member.
    */
  def getLatestKnownBalance(member: Member): FutureUnlessShutdown[Option[TrafficBalance]] = {
    FutureUnlessShutdown.outcomeF(
      trafficBalances.get(member).map(_.trafficBalances.values.lastOption)
    )
  }

  /** Return the traffic balance valid at the given timestamp for the given member.
    * Balances are cached in this class, according to the cache size defined in trafficConfig.
    * Requesting balances for timestamps outside of the cache will trigger a lookup in the database.
    * The balances in this class are updated after they've been processed and validated by the sequencer client,
    * however they can be queried on the sequencer read path. To avoid providing incorrect balance values while
    * balance updates are "in-flight" (as in being processed by the sequencer client), the parameter lastSeen0 can be
    * provided. This parameter indicates to the function the youngest timestamp for which the caller thinks a balance update might have been sequenced.
    * This allows this function to compare this timestamp with the internal state, and if necessary, wait for the potential update before
    * providing the balance.
    * Note that if requesting a traffic balance at a timestamp that has already been pruned, the function will return an error.
    * @param member member to request the traffic balance for
    * @param desired the timestamp for which the balance is requested
    * @param lastSeenO the youngest timestamp for which the caller thinks a balance update might have been sequenced
    * @param warnIfApproximate if no lastSeen0 is provided, and the desired timestamp is more recent than the last update, a warning will be logged if this is true
    * @return the traffic balance valid at the given timestamp for the given member, or empty if no balance exists for that member
    */
  def getTrafficBalanceAt(
      member: Member,
      desired: CantonTimestamp,
      lastSeenO: Option[CantonTimestamp] = None,
      warnIfApproximate: Boolean = true,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficBalanceManagerError, Option[TrafficBalance]] = {
    val lastUpdate = lastUpdateAt.get()
    val result = (lastUpdate, lastSeenO) match {
      // Desired timestamp is before or equal the timestamp just after last update, so the balance is correct and we can provide it immediately
      // This is correct because an update only becomes valid at the immediateSuccessor of its sequencing timestamp.
      // So if we ask for t1.immediateSuccessor, and the last update we saw was at t1, any further update will be at least at t1.immediateSuccessor, and therefore not relevant for t1.immediateSuccessor
      case (Some(lastUpdate), _) if desired <= lastUpdate.immediateSuccessor =>
        getBalanceAt(member, desired)
      // Desired timestamp is after the last update, but the last seen timestamp is older than the last update.
      // This means there is no balance update in "transit" waiting to be processed, so we can safely provide the balance immediately
      case (Some(lastUpdate), Some(lastSeen)) if lastSeen <= lastUpdate =>
        getBalanceAt(member, desired)
      // Here we can't be sure there's no update in transit because no lastSeenO was provided.
      // If warnIfApproximate is true, we log a warning, but still provide the balance.
      case (lastUpdate, None) =>
        if (warnIfApproximate)
          logger.warn(
            s"The desired timestamp $desired is more recent than the last update ${lastUpdate.map(_.toString).getOrElse("N/A")}," +
              s" and no 'lastSeen' timestamp was provided. The provided balance may not be up to date if a balance update is being processed."
          )
        getBalanceAt(member, desired)
      case (_, Some(lastSeen)) =>
        val promiseO = makePromiseForBalance(member, desired, lastSeen)
        promiseO.map(promise => EitherT(promise.futureUS)).getOrElse(getBalanceAt(member, desired))
    }
    result.flatTap { balance =>
      logger.trace(
        s"Providing balance for $member at $desired with lastSeen = $lastSeenO. Last update: $lastUpdate. Balance is $balance"
      )
      EitherT.pure(())
    }
  }

  private[traffic] def makePromiseForBalance(
      member: Member,
      desired: CantonTimestamp,
      lastSeen: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Option[PromiseUnlessShutdown[Either[TrafficBalanceManagerError, Option[TrafficBalance]]]] = {
    blocking {
      pendingBalanceUpdates.synchronized {
        // We need to check again here (specifically inside the synchronized block on pendingBalanceUpdates) if we haven't received an update between the beginning of the function and now.
        // By getting lastUpdateAt in the synchronized block we're sure that we won't be dequeueing pending updates concurrently, which could lead to dequeuing before we had a chance to enqueue this promise,
        // possibly leaving it in the queue forever if we don't receive anymore updates.
        val possiblyNewLastUpdate = lastUpdateAt.get()
        if (possiblyNewLastUpdate.exists(_ >= lastSeen)) {
          // We got the update in the meantime, so respond with the balance
          logger.trace(
            s"Got an update during getTrafficBalanceAt that satisfies the requested timestamp. Desired = $desired, lastSeen = $lastSeen, lastUpdatedAt = $possiblyNewLastUpdate. Responding with the balance."
          )
          None
        } else {
          logger.trace(
            s"Balance for $member at $desired is not available yet. Waiting to observe an event at $lastSeen. Last update: $possiblyNewLastUpdate"
          )
          val promise = mkPromise[Either[TrafficBalanceManagerError, Option[TrafficBalance]]](
            s"waiting for traffic balance for $member at $lastSeen, requested timestamp: $desired, last update: $possiblyNewLastUpdate",
            futureSupervisor,
          )
          val pendingUpdate = PendingBalanceUpdate(desired, lastSeen, member, promise)
          pendingBalanceUpdates.addOne(pendingUpdate).discard
          Some(promise)
        }
      }
    }
  }

  /** Mark all timestamps below (excluding) the provided one as safe to prune.
    */
  def setSafeToPruneBeforeExclusive(timestamp: CantonTimestamp): Unit = {
    safeToPruneBeforeExclusive.set(Some(timestamp))
  }

  /*
   * Merely mark all members in the cache as eligible for pruning at a timestamp calculated from the pruning config
   * The returned future will complete either when pruning is stopped manually or when the manager is closed.
   */
  private def scheduleAutoPruning(promise: PromiseUnlessShutdown[Unit])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val nextPruningAt = clock.now.plus(trafficConfig.pruningRetentionWindow.asJava)
    val resultFUS = promise.futureUS
    val scheduledFUS = clock.scheduleAt(
      ts => {
        safeToPruneBeforeExclusive.get().foreach { safeToPruneTs =>
          val threshold = ts.minus(trafficConfig.pruningRetentionWindow.asJava).min(safeToPruneTs)
          logger.debug(s"Traffic balances older than $threshold are now eligible for pruning")
          trafficBalances.underlying
            .synchronous()
            .asMap()
            .forEach((_, balances) => balances.eligibleForPruningBefore.set(Some(threshold)))
        }
      },
      nextPruningAt,
    )

    FutureUnlessShutdown(Future.firstCompletedOf(Seq(resultFUS, scheduledFUS).map(_.unwrap)))
      .flatMap {
        // If the return promise was completed, we can stop
        case result if promise.isCompleted => FutureUnlessShutdown.pure(result)
        // Otherwise we recurse
        case _ => scheduleAutoPruning(promise)
      }
  }

  /** Starts auto pruning of traffic balances according to the pruning configuration.
    */
  def startAutoPruning(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    // This future will only complete when auto pruning is stopped manually or the node goes down
    // so use the noop supervisor to avoid logging continuously that the future is not done
    lazy val newPromise =
      mkPromise[Unit]("auto pruning started", FutureSupervisor.Noop)
    autoPruningPromise.getAndUpdate({
      case None => Some(newPromise)
      case existing => existing
    }) match {
      case None => scheduleAutoPruning(newPromise)
      case Some(existing) => existing.futureUS
    }
  }

  /** Stop auto pruning of traffic balances.
    */
  def stopAutoPruning(): Unit = {
    autoPruningPromise.getAndSet(None).foreach(_.trySuccess(UnlessShutdown.unit))
  }

  override def onClosed(): Unit = {
    autoPruningPromise.getAndSet(None).foreach(_.shutdown())
  }
}

object TrafficBalanceManager {
  sealed trait TrafficBalanceManagerError extends Product with Serializable
  final case class TrafficBalanceAlreadyPruned(member: Member, timestamp: CantonTimestamp)
      extends TrafficBalanceManagerError

  /** Internal class to store traffic balances per member in an in memory cache
    * @param trafficBalances balances, sorted by timestamp in a sorted map
    *                        This allows to easily find the correct balance for a given timestamp
    * @param eligibleForPruningBefore If set, this member is eligible to be pruned for balances older than this timestamp
    */
  private final case class BalancesForMember(
      trafficBalances: SortedMap[CantonTimestamp, TrafficBalance],
      eligibleForPruningBefore: AtomicReference[Option[CantonTimestamp]] = new AtomicReference(None),
  )

  /** Internal class holding a promise for a request for a balance value at a timestamp that isn't available yet
    * @param desired requested timestamp
    * @param lastSeen timestamp for which to wait before delivering the balance
    * @param member member for which the balance is requested
    * @param promise promise to complete with the balance once it's available
    */
  private final case class PendingBalanceUpdate(
      desired: CantonTimestamp,
      lastSeen: CantonTimestamp,
      member: Member,
      promise: PromiseUnlessShutdown[Either[TrafficBalanceManagerError, Option[TrafficBalance]]],
  )
}
