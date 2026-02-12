// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.cache

import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.metrics.CacheMetrics
import com.digitalasset.canton.topology.cache.TopologyStateWriteThroughCache.CacheItem.{
  Loaded,
  Loading,
}
import com.digitalasset.canton.topology.cache.TopologyStateWriteThroughCache.{
  CacheItem,
  MaybeUpdatedTx,
  StateData,
  StateKey,
  StateKeyTuple,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.TopologyStore.StateKeyFetch
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  TopologyStore,
  TopologyStoreId,
  TopologyTransactionRejection,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.{
  TopologyChangeOp,
  TopologyMapping,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{Namespace, PhysicalSynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil, FutureUtil, MonadUtil, Mutex}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.{nowarn, tailrec}
import scala.collection.concurrent.TrieMap
import scala.collection.{immutable, mutable}
import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*
import scala.util.Try

trait TopologyStateLookupByNamespace {

  def synchronizerId: Option[PhysicalSynchronizerId]

  /** Lookup state (excludes proposals) for the given namespace
    *
    * This function only returns the state for the given namespace (namespace delegations), where
    * TopologyMapping.maybeUid is None.
    */
  def lookupForNamespace(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      ns: Namespace,
      transactionTypes: Set[Code],
      // TODO(#29400) clarify if Remove is even needed (don't think so). Otherwise, simplify API
      op: TopologyChangeOp = TopologyChangeOp.Replace,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]]

  def lookupForNamespaces(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      ns: NonEmpty[Seq[Namespace]],
      transactionTypes: Set[Code],
      op: TopologyChangeOp = TopologyChangeOp.Replace,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Map[Namespace, Seq[GenericStoredTopologyTransaction]]]

}

class StoreBasedTopologyStateLookupByNamespace(store: TopologyStore[TopologyStoreId])(implicit
    executionContext: ExecutionContext
) extends TopologyStateLookupByNamespace {

  override def synchronizerId: Option[PhysicalSynchronizerId] = store.storeId.forSynchronizer

  override def lookupForNamespace(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      ns: Namespace,
      transactionTypes: Set[Code],
      op: TopologyChangeOp,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] =
    lookupForNamespaces(
      asOf,
      asOfInclusive,
      NonEmpty.mk(Seq, ns),
      transactionTypes,
      op,
      warnIfUncached,
    )
      .map(_.toSeq.flatMap(_._2))

  override def lookupForNamespaces(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      ns: NonEmpty[Seq[Namespace]],
      transactionTypes: Set[Code],
      op: TopologyChangeOp,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Map[Namespace, Seq[GenericStoredTopologyTransaction]]] =
    op match {
      case TopologyChangeOp.Replace =>
        store
          .findPositiveTransactions(
            asOf = asOf.value,
            asOfInclusive = asOfInclusive,
            isProposal = false,
            types = transactionTypes.toSeq,
            filterUid = None,
            filterNamespace = Some(ns),
            pagination = None,
          )
          .map(_.result.groupBy(_.transaction.mapping.namespace))
      case TopologyChangeOp.Remove =>
        store
          .findNegativeTransactions(
            asOf = asOf.value,
            asOfInclusive = asOfInclusive,
            isProposal = false,
            types = transactionTypes.toSeq,
            filterUid = None,
            filterNamespace = Some(ns),
          )
          .map(_.result.groupBy(_.transaction.mapping.namespace))
    }

}

trait TopologyStateLookup extends TopologyStateLookupByNamespace {

  /** Lookup state (excludes proposals) for the given uid */
  def lookupForUid(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      uid: UniqueIdentifier,
      transactionTypes: Set[Code],
      op: TopologyChangeOp = TopologyChangeOp.Replace,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]]

  /** Lookup the history of an uid (excluding proposals) up to the given timestamp */
  @deprecated(
    since = "3.4.0",
    message =
      "fetching history is an anti-pattern with pruning and scalability as history is potentially unbounded. Do not add more tech-debt.",
  )
  def lookupHistoryForUid(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      uid: UniqueIdentifier,
      transactionType: Code,
      op: TopologyChangeOp = TopologyChangeOp.Replace,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]]

  def lookupForUids(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      uid: NonEmpty[Seq[UniqueIdentifier]],
      transactionTypes: Set[Code],
      op: TopologyChangeOp = TopologyChangeOp.Replace,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Map[UniqueIdentifier, Seq[GenericStoredTopologyTransaction]]] = MonadUtil
    .sequentialTraverse(uid)(
      lookupForUid(asOf, asOfInclusive, _, transactionTypes, op, warnIfUncached)
    )
    .map(seq => uid.toSeq.zip(seq).toMap)

}

/** Write through cache for topology state processing
  *
  * This cache groups the topology transactions from the perspective of the UID => Seq[Txs] (order
  * from latest to oldest). It is used by the state processor to add new pending transactions during
  * processing and present a consistent view of stored + processed but not yet stored topology
  * transactions.
  *
  * Due to this capability, it can also be used directly to feed the topology client, such that
  * processed topology transactions can be immediately accessed without reading from the db.
  */
class TopologyStateWriteThroughCache(
    store: TopologyStore[TopologyStoreId],
    aggregatorConfig: BatchAggregatorConfig,
    cacheEvictionThreshold: PositiveInt,
    maxCacheSize: PositiveInt,
    enableConsistencyChecks: Boolean,
    metrics: CacheMetrics,
    supervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyStateLookup
    with NamedLogging
    with FlagCloseable
    with HasCloseContext {

  private implicit val mc: MetricsContext = {
    val name = store.storeId match {
      case TopologyStoreId.SynchronizerStore(psid) =>
        psid.uid.identifier.str + "::" + psid.uid.namespace.unwrap.take(12)
      case TopologyStoreId.AuthorizedStore => "authorized"
      case TopologyStoreId.TemporaryStore(name) => "temp-" + name
    }
    MetricsContext("store" -> name, "segment" -> "head")
  }
  private val historyMC = mc.withExtraLabels("segment" -> "history")

  // maintain last successfully processed timestamp
  private val asOf = new AtomicReference[EffectiveTime](EffectiveTime.MinValue)
  private val lock = new Mutex()
  // double ended queue with cached items. oldest items are at the beginning, newest items are at the end
  private val cachedKeys = mutable.Queue[StateKey]()
  metrics.registerSizeGauge(() => lock.exclusive(cachedKeys.size.toLong))
  // list of newly fetched states. these states will be appended to the cached queue during the next eviction cycle
  private val fresh = new AtomicReference[(Int, List[StateKey])]((0, List.empty))
  // the actual state cache (UID, Code) => Seq[Tx]
  private val stateCache = new TrieMap[StateKey, CacheItem]()
  // tracking the pending additions and removals such that they can subsequently be flushed to the db
  private val pendingAdd = new AtomicReference[List[MaybeUpdatedTx]](List.empty)
  private val pendingRemove = new AtomicReference[Set[MaybeUpdatedTx]](Set.empty)
  // the eviction lock used to synchronise on evictions
  // state processing doesn't like concurrent evictions
  private val evictionLock =
    new AtomicReference[FutureUnlessShutdown[Unit]](FutureUnlessShutdown.unit)
  // we use this to detect races in the eviction logic due to programming errors
  private val detectRacingEviction = new AtomicBoolean(false)

  // parallel loading for topology lookups
  private val aggregator = BatchAggregator(
    new BatchAggregator.Processor[StateKeyFetch, Seq[
      GenericStoredTopologyTransaction
    ]]() {
      override def kind: String = "topology-loader"
      override def logger: TracedLogger = TopologyStateWriteThroughCache.this.logger
      override def executeBatch(items: NonEmpty[Seq[Traced[StateKeyFetch]]])(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): FutureUnlessShutdown[immutable.Iterable[Seq[GenericStoredTopologyTransaction]]] = {
        def reorderByValidUntil(
            txs: Seq[GenericStoredTopologyTransaction]
        ): Seq[GenericStoredTopologyTransaction] = {
          if (enableConsistencyChecks) {
            txs.sliding(2).filter(_.sizeIs == 2).foreach { items =>
              val ts0 = items(0).validUntil.map(_.value).getOrElse(CantonTimestamp.MaxValue)
              val ts1 = items(1).validUntil.map(_.value).getOrElse(CantonTimestamp.MaxValue)
              ErrorUtil.requireState(
                ts0 >= ts1,
                "Not sorted correctly\n " + items,
              )
            }
          }
          txs
            .sortBy(c =>
              (c.validUntil.map(_.value).getOrElse(CantonTimestamp.MaxValue), c.transaction.serial)
            )
            .reverse
        }

        val requestedItems = items.map(_.value)
        // first, we group all queries by state key to dedup requests
        val groupedByKey = requestedItems.groupMap1(StateKey.apply)(_.validUntilCutoff)
        // then we figure out the min effective time requested per key
        val keyToValidUntil = groupedByKey.view.mapValues(_.min1)
        // now, we map the requests into the loading tuple
        val toLoad = keyToValidUntil.map { case (key, validUntil) =>
          key.toStateKeyFetch(validUntil)
        }.toSeq
        store.fetchAllDescending(toLoad).map { loaded =>
          // group result by state key (we just get a list of transactions)
          val keyToLoadedTxs = loaded.result.groupBy(tx => StateKey(tx.mapping))
          // now, assemble result for each fetch request
          requestedItems.map { stateKeyFetch =>
            val stateKey = StateKey(stateKeyFetch)
            // get tx for this state key
            val txs = keyToLoadedTxs.getOrElse(stateKey, Seq.empty)
            // only retain transactions after the cut off timestamp
            val txsCut = txs.filter(tx => tx.validUntil.forall(_ >= stateKeyFetch.validUntilCutoff))
            reorderByValidUntil(txsCut)
          }
        }
      }
      override def prettyItem: Pretty[StateKeyFetch] = StateKeyFetch.pretty
    },
    aggregatorConfig,
  )

  private def fetchHistory(key: StateKey)(
      validUntilCutOff: EffectiveTime
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] = {
    metrics.missCount.inc()(historyMC)
    implicit val tc: TraceContext = TraceContext.empty
    aggregator.run(key.toStateKeyFetch(validUntilCutOff))
  }

  override def synchronizerId: Option[PhysicalSynchronizerId] = store.storeId.forSynchronizer

  /** Allocate eviction lock
    *
    * The returned future allows to wait until an eviction has finished The promise needs to be
    * completed when the eviction is complete.
    */
  def acquireEvictionLock()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[PromiseUnlessShutdown[Unit]] = {
    val lock = PromiseUnlessShutdown.unsupervised[Unit]()
    supervisor.supervisedUS("acquire-topology-eviction-lock")(
      evictionLock
        .getAndSet(lock.futureUS)
        .transformIntoSuccess(_ => UnlessShutdown.Outcome(lock))
    )
  }

  @VisibleForTesting
  def cacheSize(): (Int, Int) = lock.exclusive {
    (this.cachedKeys.size, fresh.get()._1)
  }

  @VisibleForTesting
  def evictIfNecessary()(implicit traceContext: TraceContext): Unit =
    // optimistically only start eviction if it isn't already running
    // doesn't matter much if we start it twice
    if (evictionLock.get().isCompleted) {
      val size = lock.exclusive(cachedKeys.size) + fresh.get()._1
      if (size > maxCacheSize.value + cacheEvictionThreshold.value) {
        logger.debug(s"Topology cache size is $size, starting eviction run")
        FutureUtil.doNotAwait(
          acquireEvictionLock()
            .map { promise =>
              promise.complete(Try(Outcome(evict().discard)))
              ()
            }
            .onShutdown(()),
          "topology cache eviction",
        )
      }
    }

  /** Evict unused items from cache
    *
    * The method is synchronized for safety, but should normally only be called from within other
    * synchronized blocks in the topology state processor.
    *
    * During eviction, we will remove enough cached items to reduce the cache size below the target
    * value. We may temporarily exceed the target value during topology state processing.
    *
    * The eviction will pull from the head of the cache queue. If the element was recently accessed,
    * it will be re-added to the end of the queue (and marked as "not recently accessed").
    * Otherwise, it will be removed from the state cache.
    */
  def evict()(implicit traceContext: TraceContext): Int = {
    ErrorUtil.requireState(
      detectRacingEviction.compareAndSet(false, true),
      "concurrent eviction running?",
    )
    lock.exclusive {
      @tailrec
      def go(todo: Int, index: Int, kept: Int, maxIdx: Int): (Int, Int) = if (
        todo > 0 && index < maxIdx
      ) {
        val next = cachedKeys.dequeue()
        val removed = stateCache.get(next) match {
          case Some(_: Loading) =>
            // this should not happen as only loaded items should be in the eviction queue
            logger.error(
              s"Element in cache $next is still loading during eviction? This should not be possible"
            )
            cachedKeys.append(next)
            false
          case Some(x: Loaded) =>
            val accessed = x.accessed.getAndSet(false)
            // if we recently accessed it, we'll keep it
            if (accessed) {
              cachedKeys.append(next)
              false
            } else {
              // removing entry from cache (the entry remains in memory and can still be processed by a concurrent thread
              // that just accessed it). a new read will just re-add the entry to the stateCache (and subsequently to fresh)
              stateCache.remove(next).isDefined
            }
          case None =>
            logger.error(
              s"Invalid concurrent cache eviction running? Tried to access $next but it was gone!"
            )
            false
        }
        go(if (removed) todo - 1 else todo, index + 1, if (!removed) kept + 1 else kept, maxIdx)
      } else (todo, kept)

      val (newItemsSize, newItems) = fresh.getAndSet((0, List.empty))
      val oldSize = cachedKeys.size // constant time op
      val numToRemove = oldSize + newItemsSize - maxCacheSize.value
      // only remove if the size increase was significant enough
      if (numToRemove > 0) {
        val (excess, kept) = go(todo = numToRemove, index = 0, kept = 0, maxIdx = oldSize)
        metrics.evictionCount.inc((numToRemove - kept).toLong)
        logger.debug(
          s"Completed topology cache eviction: now=${cachedKeys.size + newItemsSize}, fresh=${newItems.size}, cached=$oldSize, kept=$kept, excess=$excess"
        )
      } else {
        logger.debug(
          s"No cache eviction necessary: now=${cachedKeys.size + newItemsSize}, fresh=${newItems.size}, cached=$oldSize"
        )
      }
      cachedKeys.appendAll(newItems)
      ErrorUtil.requireState(
        detectRacingEviction.compareAndSet(true, false),
        "eviction was running but was not?",
      )
      cachedKeys.size
    }
  }

  /** Write pending changes to the database
    *
    * Not thread safe. We need to call this before changing the sequenced / effective time.
    */
  def flush(sequenced: SequencedTime, effective: EffectiveTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    asOf.set(effective)
    val toAdd = pendingAdd.getAndSet(List.empty)
    val toRemove = pendingRemove.getAndSet(Set.empty)
    // TODO (#29400) we can change this to pointwise queries as we really now know which transaction needs
    //   to be updated. the current logic was kept so far to minimise the change size.
    //   If we change the interface to the topology store, we could actually remove the sequenced / effective
    //   times here and write data for multiple timestamps in one go.
    val mappedRemoves =
      toRemove.groupBy(_.stored.mapping.uniqueKey).map { case (mappingHash, txs) =>
        // find max serial among removed
        val maxSerial =
          txs.filterNot(_.stored.transaction.isProposal).map(_.stored.serial).maxOption
        mappingHash -> (
          maxSerial,
          txs
            .filter(tx =>
              tx.stored.transaction.isProposal && maxSerial.forall(_ < tx.stored.serial)
            )
            .map(_.stored.transaction.transaction.hash),
        )
      }

    store
      .update(
        sequenced,
        effective,
        mappedRemoves,
        toAdd.reverse.map(_.toValidated), // insert preserving original order
      )
      .map { _ =>
        toAdd.foreach { added =>
          // mark transaction as persisted. this means that if validUntil is set subsequently,
          // it will be processed via the pendingRemoves container
          added.persisted.set(true)
        }
        val updatedKeys = toAdd.map(_.stateKey) ++ toRemove.map(_.stateKey)
        updatedKeys.foreach(c =>
          stateCache.get(c).foreach {
            case _: Loading =>
              logger.error(
                "Found a state cache entry being loaded while there are concurrent writes happening"
              )
            case loaded: Loaded =>
              loaded.transferArchivedToTail(enableConsistencyChecks)
          }
        )
      }
  }

  /** Appending a transaction to the cache
    *
    * Not thread safe. Only call from within state processor (sequentially).
    *
    * We will add the transaction to the cache and queue it for writing. It will only be written
    * once flush() is called.
    *
    * Note we expect that all appends have the same sequenced and effective time until flushed.
    */
  def append(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      tx: GenericValidatedTopologyTransaction,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val stateKey = StateKey(tx.transaction.mapping)
    // get will not go to the db, as we pre-fetched the rows during state processing
    get(stateKey, effective.value, warnIfUncached = true).map { state =>
      val toStore = StoredTopologyTransaction(
        sequenced,
        validFrom = effective,
        validUntil = Option.when(tx.rejectionReason.isDefined || tx.expireImmediately)(effective),
        transaction = tx.transaction,
        rejectionReason = tx.rejectionReason.map(_.asString300),
      )
      val stateData = state.currentState
      // this cannot happen as we ensure that the data is recent enough during fetch(...)
      ErrorUtil.requireState(
        stateData.validUntilInclusive.value <= effective.value,
        s"Current state is too recent for $effective $stateData",
      )
      logger.debug(s"Adding $toStore")
      def expire(toExpire: MaybeUpdatedTx): Unit = {
        val before = toExpire.expireAt(effective)
        logger.debug("Marking transaction as expired:" + toExpire)
        ErrorUtil.requireState(
          before.isEmpty,
          s"Trying to expire an already expired transaction $toExpire because of\n   $tx",
        )
        // if tx was already persisted, we register the expiration as a remove
        if (toExpire.persisted.get()) {
          pendingRemove.getAndUpdate(_ + toExpire).discard
        } else {
          // otherwise, the tx must still be queued up as a pending add
          if (enableConsistencyChecks) {
            ErrorUtil.requireState(
              pendingAdd.get().contains(toExpire),
              s"Trying to expire not-persisted tx $toExpire because of $tx but can't find it among pending adds",
            )
          }
        }
      }

      // archive previous proposal
      if (tx.rejectionReason.isEmpty && tx.transaction.isProposal) {
        stateData
          .findAtMostOne(
            existing =>
              existing.transaction.isProposal && existing.validUntil.isEmpty &&
                existing.transaction.transaction.hash == tx.transaction.transaction.hash,
            enableConsistencyChecks,
            "append-archive-existing-proposal",
          )
          .foreach(expire)
      }
      // archive previous transaction and all prior proposals
      else if (tx.rejectionReason.isEmpty) {
        // remove existing txs
        // UPDATE tx SET valid_until = effective WHERE storeId = XYZ
        //    AND valid_until is NULL and valid_from < effective
        val existing = stateData.findAtMostOne(
          existing =>
            !existing.transaction.isProposal && existing.validUntil.isEmpty && existing.mapping.uniqueKey == tx.mapping.uniqueKey,
          enableConsistencyChecks,
          "append-archive-existing-tx",
        )
        existing match {
          case None if tx.transaction.serial == PositiveInt.one => () // Nothing to archive
          case None =>
            // This case might happen if somebody added an authorized transaction with serial > 1
            // It is odd but supported
            logger.debug("No existing transaction to expire for " + tx.transaction)
          case Some(toExpire) =>
            ErrorUtil.requireState(
              toExpire.stored.serial.value <= tx.serial.value,
              s"Mismatched serials when expiring existing transaction $toExpire because of\n   $tx",
            )
            expire(toExpire)
        }
        // expire all proposals for this mapping
        stateData.head
          .filter(existing =>
            existing.isActive && existing.stored.transaction.isProposal && existing.stored.mapping.uniqueKey == tx.mapping.uniqueKey &&
              existing.stored.serial <= tx.serial // (only proposals up to and including the serial of the accepted tx)
          )
          .foreach(expire)
      }

      // add transaction to state
      val maybeUpdatedTx =
        new MaybeUpdatedTx(
          stateKey,
          toStore,
          initPersisted = false,
          typedRejectionReasonOfNewlyAppendedTxs = tx.rejectionReason,
        )
      state.addNewTransaction(maybeUpdatedTx)
      pendingAdd.getAndUpdate(maybeUpdatedTx :: _).discard
    }

  }

  private def appendToFresh(keys: List[StateKey]): List[StateKey] =
    fresh.getAndUpdate { case (count, lst) =>
      ((count + keys.size), keys ++ lst)
    }._2

  /** Fetch the provided uids and namespaces and ensure their data is loaded in memory
    *
    * Crash recovery behaviour: If we resume on a restart or crash, we will find previously
    * processed transactions in the store. Here, we choose the strategy to keep the data in the
    * store, relying on idempotent inserts, but we drop all changes that happened in the future.
    */
  def fetch(
      effective: EffectiveTime,
      toLoad: Set[StateKeyTuple],
      storeIsEmpty: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {

    val keys: Set[StateKey] = toLoad.map { case (code, ns, idO) => StateKey(code, ns, idO) }
    // if we know that the story is empty, bypass the db lookup
    if (storeIsEmpty) {
      keys.foreach(key =>
        stateCache
          .putIfAbsent(
            key,
            Loaded(
              StateData(key, Seq.empty, Seq.empty, effective),
              fetchHistory(key),
            ),
          )
          .discard
      )
      appendToFresh(keys.toList).discard
      FutureUnlessShutdown.unit
    } else {

      val started = System.nanoTime()
      // we either need to load the data or we need to check that we have enough history available
      val (prepared, postload) =
        keys.toSeq.partitionMap(key => getOrPrepareLoading(key).bimap(key -> _, key -> _))

      // check and reset existing data (should not really happen as nothing should
      // access the cache prior to fetch being in place and working (asOf is incremented in append)
      // but to avoid future mistakes, we perform the sanity checks here
      val checkedF = MonadUtil.parTraverseWithLimit_(PositiveInt.tryCreate(100))(postload) {
        case (_, fut) =>
          fut.flatMap { loaded =>
            // trigger loading of fetch history
            loaded.dropPendingChanges(effective, enableConsistencyChecks)
          }
      }

      // load missing data
      val prepareF = NonEmpty
        .from(prepared.toList)
        .map { toLoad =>
          metrics.missCount.inc(toLoad.length.toLong)
          MonadUtil
            .parTraverseWithLimit(PositiveInt.tryCreate(100))(toLoad) { case (key, promise) =>
              aggregator.run(key.toStateKeyFetch(effective)).map { txs =>
                // Drop the pending changes: if we recover from a crash during topology processing,
                // we might load transactions which we are going to reprocess now. We deal with this by simply
                // just dropping any modification of the future and resetting the state to just before this timestamp
                val state =
                  StateData
                    .fromLoaded(key, txs, effective, enableConsistencyChecks)
                    .dropPendingChanges(effective.value)
                val loaded = Loaded(state, fetchHistory(key))
                stateCache.put(key, loaded).discard
                completePromise(promise, loaded)
                (key, txs.size)
              }
            }
            .map { loaded =>
              if (logger.underlying.isDebugEnabled && prepared.nonEmpty) {

                val hits = (keys -- prepared.map(_._1)).map(s => s"hit:  $s").view
                val misses = loaded.map { case (key, count) =>
                  s"miss: $key, #$count"
                }.view
                logger.debug(
                  s"Prefetching into topology cache (${java.util.concurrent.TimeUnit.NANOSECONDS
                      .toMillis(System.nanoTime() - started)} ms):\n  " +
                    (hits ++ misses).mkString("\n  ")
                )
              }
              val loadedKeys = toLoad.map(_._1).forgetNE
              // see comments in get(...)
              if (enableConsistencyChecks) lock.exclusive {
                val previous = appendToFresh(loadedKeys).toSet
                val loadedKeysSet = loadedKeys.toSet
                val alreadyKnown = previous.intersect(loadedKeysSet)
                ErrorUtil.requireState(
                  alreadyKnown.isEmpty,
                  s"Detected concurrent loading of items into the fresh cache $alreadyKnown",
                )
                val alreadyCached = cachedKeys.toSet.intersect(loadedKeysSet)
                ErrorUtil.requireState(
                  alreadyCached.isEmpty,
                  s"Detected concurrent loading of items into the cache $alreadyCached",
                )
              }
              else {
                appendToFresh(loadedKeys).discard
              }
              ()
            }
        }
        .getOrElse(FutureUnlessShutdown.unit)
      for {
        _ <- checkedF
        _ <- prepareF
      } yield ()
    }
  }

  private def completePromise(
      promise: PromiseUnlessShutdown[CacheItem.Loaded],
      loaded: CacheItem.Loaded,
  )(implicit traceContext: TraceContext): Unit =
    ErrorUtil.requireState(
      promise.outcome(loaded),
      "Load promise was already completed. This should not be possible",
    )

  private def getOrPrepareLoading(
      key: StateKey
  ): Either[PromiseUnlessShutdown[CacheItem.Loaded], FutureUnlessShutdown[
    CacheItem.Loaded
  ]] =
    // We are missing an getAndUpdate here. As we want to avoid creating too many unused
    // promises, we'll use the "get / putIfAbsent" which will only create a potentially unused
    // promise if we really race
    stateCache.get(key) match {
      // we have loaded it already, or we are currently loading it
      case Some(present) =>
        metrics.hitCount.inc()
        Right(present.value)
      // init loading
      case None =>
        val promise = PromiseUnlessShutdown.unsupervised[CacheItem.Loaded]()
        stateCache.putIfAbsent(key, CacheItem.Loading(promise)) match {
          // if we raced and lost, return the other one
          case Some(other) =>
            metrics.hitCount.inc()
            Right(other.value)
          // otherwise, start loading
          case None =>
            Left(promise) // metric will be increased elsewhere
        }
    }

  protected def get(
      key: StateKey,
      asOfInclusive: CantonTimestamp,
      warnIfUncached: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[CacheItem.Loaded] = {
    getOrPrepareLoading(key).leftMap { promise =>
      metrics.missCount.inc()
      if (warnIfUncached) {
        // If someone adds a mapping check without adding it to the state processor prefetch,
        // we'll detect this and emit an error.
        logger.warn(
          s"Trying to access topology state for $key but cannot find it in cache. Please inform your developer to add the prefetch dependency to TopologyStateProcessor.preloadCaches. Things are still correct, just slower."
        )
      }
      val validUntilInclusive = EffectiveTime(asOfInclusive)
      // start eviction if necessary. we only need to start it if we had a cache miss
      evictIfNecessary()
      aggregator.run(key.toStateKeyFetch(validUntilInclusive)).map { txs =>
        val loaded = Loaded(
          StateData.fromLoaded(key, txs, validUntilInclusive, enableConsistencyChecks),
          fetchHistory(key),
        )
        stateCache.put(key, loaded).discard
        completePromise(promise, loaded)
        // mark this as loaded (this will schedule its eviction later on)
        // loading cannot race with eviction as only items which we have added to fresh can be evicted
        // but we only add them to fresh once they are successfully loaded.
        if (enableConsistencyChecks) lock.exclusive {
          val previous = appendToFresh(List(key))
          // Validate that nothing raced with us. We should not race with anything as the
          // access via get(..) should guarantee that the same key is not loaded concurrently.
          // Eviction only happens via fresh / cached, which means we can use that to detect whether
          // there is a bug in our cache loading & eviction logic.
          ErrorUtil.requireState(
            !previous.contains(key),
            s"Detected concurrent loading of items into the fresh cache $key",
          )
          ErrorUtil.requireState(
            !cachedKeys.contains(key),
            s"Detected concurrent loading of items into the cache $key",
          )
        }
        else {
          appendToFresh(List(key)).discard
        }
        loaded
      }
    }
  }.merge
    // finally make sure that the data segment we are returning includes what we are looking for
    .flatMap(loaded =>
      loaded
        .headOrFetchHistory(EffectiveTime(asOfInclusive), enableConsistencyChecks)
        .map(_ => loaded)
    )

  /** Drop pending
    *
    * Not thread safe. If the state processor runs as part of the synchronizer topology manager or
    * authorized topology manager, then we might have to reset the cache if we abort the writes, or
    * if we dispatch to the synchronizer. We have to reset the entire cache as we are otherwise
    * potentially missing out writes on the synchronizer
    */
  def dropPending(): Unit =
    lock.exclusive {
      pendingAdd.set(List.empty)
      pendingRemove.set(Set.empty)
      cachedKeys.clear()
      fresh.set((0, List.empty))
      stateCache.clear()
    }

  override def lookupForUid(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      uid: UniqueIdentifier,
      transactionTypes: Set[Code],
      op: TopologyChangeOp,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] =
    transactionTypes.toSeq
      .map(StateKey(_, uid))
      // this is safe as transaction types is an enum with a few elements
      // plus we batch within the batch aggregator
      .parFlatTraverse(
        get(_, asOf.value, warnIfUncached).map(_.currentState.filterState(asOf, asOfInclusive, op))
      )

  @nowarn("cat=deprecation")
  override def lookupHistoryForUid(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      uid: UniqueIdentifier,
      transactionType: Code,
      op: TopologyChangeOp,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] =
    get(StateKey(transactionType, uid), asOf.value, warnIfUncached).flatMap(
      _.headOrFetchHistory(EffectiveTime(CantonTimestamp.MinValue), enableConsistencyChecks)
        .map(_.history(asOf, asOfInclusive, op))
    )

  override def lookupForNamespace(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      ns: Namespace,
      transactionTypes: Set[Code],
      op: TopologyChangeOp,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] =
    transactionTypes.toSeq
      .map(StateKey(_, ns, None))
      .parFlatTraverse(
        get(_, asOf.value, warnIfUncached).map(_.currentState.filterState(asOf, asOfInclusive, op))
      )

  override def lookupForNamespaces(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      ns: NonEmpty[Seq[Namespace]],
      transactionTypes: Set[Code],
      op: TopologyChangeOp = TopologyChangeOp.Replace,
      warnIfUncached: Boolean = false,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Map[Namespace, Seq[GenericStoredTopologyTransaction]]] =
    MonadUtil
      .sequentialTraverse(ns)(
        lookupForNamespace(asOf, asOfInclusive, _, transactionTypes, op, warnIfUncached)
      )
      .map(seq => ns.toSeq.zip(seq).toMap)

  /** Find the current transaction active for the given unique key
    *
    * Used by the [[com.digitalasset.canton.topology.processing.TopologyStateProcessor]] to get the
    * current inStore transaction.
    *
    * @param timeHint
    *   an initial time hint to have a reference time for how much history we load into the cache by
    *   default
    */
  def lookupActiveForMapping(
      timeHint: EffectiveTime,
      mapping: TopologyMapping,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] =
    get(StateKey(mapping), timeHint.value, warnIfUncached = true).flatMap { loaded =>
      // if we are checking for consistency, make sure we load all transactions
      val stateF =
        if (enableConsistencyChecks)
          loaded.headOrFetchHistory(
            EffectiveTime(CantonTimestamp.MinValue),
            enableConsistencyChecks,
          )
        else FutureUnlessShutdown.pure(loaded.currentState)
      stateF.map(_.findLastUpdateForMapping(enableConsistencyChecks, mapping))
    }

  /** Lookup the pending proposal for the given transaction
    *
    * Used by [[com.digitalasset.canton.topology.processing.TopologyStateProcessor]] to check for
    * any pending proposal for the given transaction
    */
  def lookupPendingProposal(
      timeHint: EffectiveTime,
      tx: TopologyTransaction[TopologyChangeOp, TopologyMapping],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] =
    get(StateKey(tx.mapping), timeHint.value, warnIfUncached = true).map { loaded =>
      loaded.currentState
        .findAtMostOne(
          stored =>
            tx.hash == stored.transaction.transaction.hash &&
              stored.rejectionReason.isEmpty && stored.validUntil.isEmpty && stored.transaction.isProposal,
          enableConsistencyChecks,
          s"lookup-pending-proposal for $tx",
        )
        .map(_.stored)
    }
}

object TopologyStateWriteThroughCache {

  val noOpCacheMetrics = new CacheMetrics("noop", NoOpMetricsFactory)

  type StateKeyTuple = (TopologyMapping.Code, Namespace, Option[String185])

  final private[cache] case class StateKey(
      code: TopologyMapping.Code,
      namespace: Namespace,
      identifier: Option[String185],
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[StateKey] = StateKey.pretty
    def toStateKeyFetch(
        validUntilInclusive: EffectiveTime
    ): StateKeyFetch =
      StateKeyFetch(code, namespace, identifier, validUntilInclusive)

  }

  private[cache] object StateKey {
    import PrettyUtil.*
    import com.digitalasset.canton.util.ShowUtil.*

    private val pretty: Pretty[StateKey] = prettyOfClass[StateKey](
      param("code", _.code.code.unquoted),
      param("ns", _.namespace),
      paramIfDefined("id", _.identifier.map(_.str.unquoted)),
    )

    def apply(fetch: StateKeyFetch): StateKey =
      StateKey(fetch.code, fetch.namespace, fetch.identifier)

    def apply(mapping: TopologyMapping): StateKey =
      StateKey(mapping.code, mapping.namespace, mapping.maybeUid.map(_.identifier))

    def apply(code: TopologyMapping.Code, uid: UniqueIdentifier): StateKey =
      StateKey(code, uid.namespace, uid.identifier.some)

  }

  /** Trait to express a cached item which may be loaded or still loading */
  private[cache] sealed trait CacheItem {
    def value: FutureUnlessShutdown[Loaded]
  }

  /** Cached state data
    *
    * We use the topology state write through cache for tx processing as well as for topology client
    * reading. Transactions that do not have a validUntil date are kept in head. The rest is in
    * tail.
    *
    * Tail is sorted by valid-until descending, which means that any historical access can use
    * takeWhile to limit the depth the list has to be searched.
    *
    * @param head
    *   all transactions with valid until is None or dirty removes (means we've just updated the
    *   expiry date but haven't yet moved it into the tail)
    * @param tail
    *   tail transactions with valid_until ordered DESC up until the tailValidUntilInclusive
    * @param validUntilInclusive
    *   the lowest asOf timepoint that can be served from head + tail.
    */
  private[cache] final case class StateData(
      key: StateKey,
      head: Seq[MaybeUpdatedTx],
      tail: Seq[GenericStoredTopologyTransaction],
      validUntilInclusive: EffectiveTime,
  ) extends PrettyPrinting {

    override def pretty: Pretty[StateData] = StateData.prettyInstance

    def dropPendingChanges(
        asOfInclusive: CantonTimestamp
    )(implicit loggingContext: ErrorLoggingContext): StateData = {
      ErrorUtil.requireState(
        asOfInclusive >= validUntilInclusive.value,
        s"State data is out-dated asOf=$asOfInclusive, available=${validUntilInclusive.value}",
      )
      // remove all future additions changes
      val newHead = head.filter(_.stored.validFrom.value < asOfInclusive)
      val (toHead, remainingTail) = tail
        .filter(_.validFrom.value < asOfInclusive)
        .partition(
          // all items that have a validUntil time in the future need to go to head
          _.validUntil.exists(validUntil => validUntil.value >= asOfInclusive)
        )
      // reassemble new head
      val mergedHead = newHead ++ toHead.map(tx =>
        new MaybeUpdatedTx(
          key,
          initStored = tx.copy(validUntil = None),
          initPersisted = true,
          typedRejectionReasonOfNewlyAppendedTxs = None,
        )
      )
      copy(head = mergedHead, tail = remainingTail)
    }

    /** Transfers writes into the tail */
    def transferArchivedToTail(
        enableConsistencyChecks: Boolean
    )(implicit loggingContext: ErrorLoggingContext): StateData = {
      val (stillHead, archived) = head.partition(_.stored.validUntil.isEmpty)
      if (archived.isEmpty) {
        this
      } else {
        val newTail = archived.map(_.stored) ++ tail
        if (enableConsistencyChecks) {
          newTail.sliding(2).filter(_.sizeIs == 2).foreach { items =>
            val one = items(0)
            val two = items(1)
            (one.validUntil.map(_.value), two.validUntil.map(_.value)) match {
              case (Some(oneUntil), Some(twoUntil)) if oneUntil >= twoUntil =>
              // all good
              case _ =>
                ErrorUtil.invalidState(s"Invalid order in tail:\n  " + newTail.mkString("\n  "))
            }
          }
        }
        copy(
          head = stillHead,
          tail = newTail,
        )
      }
    }

    def addNewTransaction(
        maybeUpdatedTx: MaybeUpdatedTx
    )(implicit errorLoggingContext: ErrorLoggingContext): StateData = {
      val stored = maybeUpdatedTx.stored
      ErrorUtil.requireState(
        stored.rejectionReason.isEmpty || stored.validUntil.nonEmpty,
        s"Found a rejected tx with open validity $stored",
      )
      // if the transaction is already archived, we push it to the tail
      if (stored.validUntil.isDefined) {
        copy(tail = stored +: tail)
      } else {
        copy(head = maybeUpdatedTx +: head)
      }
    }

    def updateHistorical(
        newValidUntilInclusive: EffectiveTime,
        transactions: Seq[GenericStoredTopologyTransaction],
        enableConsistencyChecks: Boolean,
    )(implicit errorLoggingContext: ErrorLoggingContext): StateData =
      if (newValidUntilInclusive < validUntilInclusive) {
        val newTail =
          tail ++ transactions.filter(_.validUntil.exists(_ < validUntilInclusive))
        if (enableConsistencyChecks) {
          val invalid = newTail.filter(tt =>
            tt.validUntil.isEmpty || StateKey(tt.mapping) != key || tt.rejectionReason.nonEmpty
          )
          ErrorUtil.requireState(
            invalid.isEmpty,
            s"Invalid historical transactions supplied of wrong key $key: $invalid",
          )
          (head.map(_.stored) ++ newTail)
            .foldLeft(Set.empty[GenericStoredTopologyTransaction]) { case (acc, elem) =>
              ErrorUtil.requireState(
                !acc.contains(elem),
                s"Found duplicate transaction in history $elem",
              )
              acc + elem
            }
            .discard
        }
        copy(
          // append all transactions that are not yet in the tail
          tail = newTail,
          validUntilInclusive = newValidUntilInclusive,
        )
      } else this

    /** find last update for mapping */
    def findLastUpdateForMapping(
        checkConsistency: Boolean,
        mapping: TopologyMapping,
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Option[GenericStoredTopologyTransaction] = {
      val inHead = findAtMostOne(
        tx =>
          tx.validUntil.isEmpty &&
            tx.mapping.uniqueKey == mapping.uniqueKey && !tx.transaction.isProposal && tx.rejectionReason.isEmpty,
        checkConsistency,
        "find-last-update-for-mapping",
      )
      ErrorUtil.requireState(inHead.forall(_.isActive), s"Found inactive in head state? $inHead")
      val inHeadStored = inHead.map(_.stored)
      // here we'll check whether there is anything dodgy in the history
      if (checkConsistency) {
        val prev = (head.map(_.stored).view ++ tail).find(tx =>
          !inHeadStored.contains(tx) &&
            tx.mapping.uniqueKey == mapping.uniqueKey && tx.rejectionReason.isEmpty && !tx.transaction.isProposal
        )
        (inHeadStored, prev) match {
          case (Some(cur), Some(old)) =>
            if (!(cur.serial == old.serial || cur.serial == old.serial.increment))
              errorLoggingContext.debug("GOING TO BOUNCE\n  " + tail.mkString("\n  "))
            ErrorUtil.requireState(
              // may be equal if late signature was added, otherwise must be an increment
              cur.serial == old.serial || cur.serial == old.serial.increment,
              s"Inconsistent head=$cur vs old=$old",
            )
          case (Some(_), None) => ()
          case (None, None) => ()
          case (None, Some(old)) =>
            ()
            ErrorUtil.invalidState(
              s"Inconsistent empty head vs tail = $old"
            )
        }
      }
      inHeadStored
    }

    /** find the transaction matching the predicate
      *
      * most read operations should find exactly one transaction. as an additional check, we can
      * also scan the entire state to ensure that there are not multiple that match by accident.
      */
    def findAtMostOne(
        filter: GenericStoredTopologyTransaction => Boolean,
        checkConsistency: Boolean,
        context: => String,
    )(implicit errorLoggingContext: ErrorLoggingContext): Option[MaybeUpdatedTx] = if (
      checkConsistency
    ) {
      val items = head.filter(mb => filter(mb.stored))
      ErrorUtil.requireState(
        items.sizeIs <= 1,
        s"Expected at most one for $context, found ${items.size}: ${items.mkString("\n  ")}",
      )
      items.headOption
    } else {
      head.find(mb => filter(mb.stored))
    }

    /** is not rejected and effective at or before the given timestamp */
    private def isEffectiveAtOrBefore(
        tx: GenericStoredTopologyTransaction,
        asOf: EffectiveTime,
        asOfInclusive: Boolean,
    ): Boolean =
      (if (asOfInclusive) {
         tx.validFrom.value <= asOf.value
       } else {
         tx.validFrom.value < asOf.value
       }) && tx.rejectionReason.isEmpty

    /** returns the history of the transactions (excluding proposals) up to the timestamp matching
      * the predicate
      *
      * fetching the history is an anti-pattern. don't do it.
      */
    @deprecated(
      since = "3.4.0",
      message = "fetching history is an anti-pattern with pruning and scalability. don't use it.",
    )
    def history(
        asOf: EffectiveTime,
        asOfInclusive: Boolean,
        op: TopologyChangeOp,
    )(implicit errorLoggingContext: ErrorLoggingContext): Seq[GenericStoredTopologyTransaction] = {
      ErrorUtil.requireState(
        validUntilInclusive.value == CantonTimestamp.MinValue,
        s"Accessing history, but history is not available for $key, ${validUntilInclusive.value}",
      )
      (head.iterator.map(_.stored) ++ tail.iterator)
        .filter(tx =>
          isEffectiveAtOrBefore(tx, asOf, asOfInclusive)
            && !tx.transaction.isProposal && op == tx.transaction.operation
        )
        .toSeq
    }

    def filterState(
        asOf: EffectiveTime,
        asOfInclusive: Boolean,
        op: TopologyChangeOp,
    )(implicit errorLoggingContext: ErrorLoggingContext): Seq[GenericStoredTopologyTransaction] = {
      ErrorUtil.requireState(
        validUntilInclusive.value <= asOf.value,
        s"Accessing history, but history is not available for $key, ${validUntilInclusive.value}",
      )
      val headAndTail = head.iterator.map(_.stored) ++
        tail.iterator.takeWhile(_.validUntil.exists(_.value >= asOf.value))
      headAndTail.filter { tx =>
        tx.isActiveAsOf(asOf = asOf, asOfInclusive = asOfInclusive) &&
        !tx.transaction.isProposal && op == tx.transaction.operation
        && tx.rejectionReason.isEmpty
      }.toSeq
    }

  }

  private[cache] object StateData {

    private val prettyInstance: Pretty[StateData] = {
      import PrettyUtil.*
      prettyOfClass[StateData](
        param("key", _.key),
        param("head", _.head.size),
        param("tail", _.tail.size),
        param("cutOff", _.validUntilInclusive.value),
      )
    }

    def fromLoaded(
        stateKey: StateKey,
        initial: Seq[GenericStoredTopologyTransaction],
        validUntilInclusive: EffectiveTime,
        checkConsistency: Boolean,
    )(implicit errorLoggingContext: ErrorLoggingContext): StateData = {
      if (checkConsistency) {
        val invalid = initial.filter { tx =>
          tx.rejectionReason.nonEmpty || StateKey(tx.mapping) != stateKey || tx.validUntil.exists(
            _ < validUntilInclusive
          )
        }
        ErrorUtil.requireState(
          invalid.isEmpty,
          s"Transactions loaded into the state key $stateKey don't seem valid:\n  " + invalid
            .mkString("\n  "),
        )
        initial
          .foldLeft(Set.empty[GenericStoredTopologyTransaction]) { case (acc, elem) =>
            if (acc.contains(elem) && !elem.validUntil.contains(elem.validFrom)) {
              ErrorUtil.invalidState(s"Found duplicate transaction in state $elem")
            }
            acc + elem
          }
          .discard
      }
      val (head, tail) = initial.partition(_.validUntil.isEmpty)
      val sortedTail =
        tail
          .sortBy(c =>
            (c.validUntil.map(_.value).getOrElse(CantonTimestamp.MaxValue), c.transaction.serial)
          )
          .reverse
      val headWithMaybe =
        head.toList.map(stored => new MaybeUpdatedTx(stateKey, stored, initPersisted = true, None))
      StateData(stateKey, headWithMaybe, sortedTail, validUntilInclusive)
    }

  }

  private[cache] object CacheItem {
    final case class Loading(promise: PromiseUnlessShutdown[Loaded]) extends CacheItem {
      override def value: FutureUnlessShutdown[Loaded] =
        promise.futureUS
    }

    /** An item loaded in the cache
      *
      * @param state
      *   the current state as loaded
      * @param fetchHistory
      *   fetch the history from present to validUntil (which is the date passed)
      */
    final case class Loaded private (
        private val state: AtomicReference[StateData],
        fetchHistory: EffectiveTime => FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]],
    ) extends CacheItem {

      def key: StateKey = state.get().key

      // track whether this has been recently accessed (via get())
      val accessed: AtomicBoolean = new AtomicBoolean(true)

      def currentState: StateData = state.get()

      private def modify(update: StateData => StateData): StateData =
        state.updateAndGet(update(_))

      def addNewTransaction(maybeUpdatedTx: MaybeUpdatedTx)(implicit
          errorLoggingContext: ErrorLoggingContext
      ): Unit =
        modify(_.addNewTransaction(maybeUpdatedTx)).discard

      def transferArchivedToTail(enableConsistencyChecks: Boolean)(implicit
          errorLoggingContext: ErrorLoggingContext
      ): Unit =
        modify(_.transferArchivedToTail(enableConsistencyChecks)).discard

      override def value: FutureUnlessShutdown[Loaded] = {
        accessed.set(true)
        FutureUnlessShutdown.pure(this)
      }

      /** return loaded state which includes a data reference */
      def headOrFetchHistory(
          asOf: EffectiveTime,
          enableConsistencyChecks: Boolean,
      )(implicit
          ec: ExecutionContext,
          elc: ErrorLoggingContext,
      ): FutureUnlessShutdown[StateData] = {
        val cur = state.get()
        // check if we can serve the data from the head data set
        if (cur.validUntilInclusive.value <= asOf.value) {
          FutureUnlessShutdown.pure(cur)
        } else {
          elc.debug(s"Extending history of $key to $asOf, as current is ${cur.validUntilInclusive}")
          // check if we can serve the data from the current history set
          fetchHistory(asOf).map { loaded =>
            // historical fetches might race, we'll just keep the longest history around
            modify(_.updateHistorical(asOf, loaded, enableConsistencyChecks))
          }
        }
      }

      def dropPendingChanges(asOfInclusive: EffectiveTime, enableConsistencyChecks: Boolean)(
          implicit
          ec: ExecutionContext,
          loggingContext: ErrorLoggingContext,
      ): FutureUnlessShutdown[Unit] =
        headOrFetchHistory(asOfInclusive, enableConsistencyChecks).map { _ =>
          // we can ignore the result of headOrFetchHistory, because it mutates the state of `Loaded.this`
          modify(_.dropPendingChanges(asOfInclusive.value)).discard
        }
    }

    object Loaded {
      def apply(
          state: StateData,
          fetchHistory: EffectiveTime => FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]],
      ): Loaded =
        Loaded(new AtomicReference(state), fetchHistory)
    }

  }

  /** A cached but mutable topology item
    *
    * @param initStored
    *   the initial value of the stored transaction
    * @param initPersisted
    *   if true then the stored transaction was loaded from db, otherwise it was added by processing
    * @param typedRejectionReasonOfNewlyAppendedTxs
    *   the rejection reason is stored in the database as a string which cannot be cast back, but
    *   the append method needs the typed version. therefore, if we add a transaction, we preserve
    *   the original typed rejection for the new transactions that are added via append(...)
    */
  private[cache] final class MaybeUpdatedTx(
      val stateKey: StateKey,
      initStored: GenericStoredTopologyTransaction,
      initPersisted: Boolean,
      typedRejectionReasonOfNewlyAppendedTxs: Option[TopologyTransactionRejection],
  ) extends PrettyPrinting {

    private val current: AtomicReference[GenericStoredTopologyTransaction] = new AtomicReference(
      initStored
    )

    /** flag to mark whether the transaction was already persisted.
      *
      * if a transaction is archived in the same batch, then we write the validUntil as part of the
      * INSERT otherwise, we write it as an UPDATE later on. we use this flag to decide which
      * strategy to use.
      */
    val persisted = new AtomicBoolean(initPersisted)
    def expireAt(timestamp: EffectiveTime): Option[EffectiveTime] =
      current.getAndUpdate(_.copy(validUntil = Some(timestamp))).validUntil

    /** true if the transaction has not yet been expired */
    def isActive: Boolean = current.get().validUntil.isEmpty
    def signed: GenericSignedTopologyTransaction = current.get().transaction

    def stored: GenericStoredTopologyTransaction = current.get()

    def toValidated: GenericValidatedTopologyTransaction = {
      require(
        stored.rejectionReason == typedRejectionReasonOfNewlyAppendedTxs.map(_.asString300),
        s"Mismatch in rejection reason for $stored vs $typedRejectionReasonOfNewlyAppendedTxs",
      )
      ValidatedTopologyTransaction(
        stored.transaction,
        typedRejectionReasonOfNewlyAppendedTxs,
        expireImmediately = stored.validUntil.nonEmpty,
      )
    }

    override protected def pretty: Pretty[MaybeUpdatedTx] = prettyOfClass(
      param("stored", _.stored),
      paramIfTrue("persisted", _.persisted.get()),
    )

  }

}
