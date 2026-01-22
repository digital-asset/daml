// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.cache

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.topology.cache.TopologyStateWriteThroughCache.CacheItem.{
  Loaded,
  Loading,
}
import com.digitalasset.canton.topology.cache.TopologyStateWriteThroughCache.StateKey.{
  StateNamespace,
  StateUid,
}
import com.digitalasset.canton.topology.cache.TopologyStateWriteThroughCache.{
  CacheItem,
  MaybeUpdatedTx,
  StateKey,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
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
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil, MonadUtil, Mutex}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.{immutable, mutable}
import scala.concurrent.ExecutionContext

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
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]]

  def lookupForNamespaces(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      ns: NonEmpty[Seq[Namespace]],
      transactionTypes: Set[Code],
      op: TopologyChangeOp = TopologyChangeOp.Replace,
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
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] =
    lookupForNamespaces(asOf, asOfInclusive, NonEmpty.mk(Seq, ns), transactionTypes, op)
      .map(_.toSeq.flatMap(_._2))

  override def lookupForNamespaces(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      ns: NonEmpty[Seq[Namespace]],
      transactionTypes: Set[Code],
      op: TopologyChangeOp,
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
            pagination = None,
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
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]]

  /** Lookup the history of a uid (excluding proposals) up to the given timestamp */
  def lookupHistoryForUid(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      uid: UniqueIdentifier,
      transactionType: Code,
      op: TopologyChangeOp = TopologyChangeOp.Replace,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]]

  def lookupForUids(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      uid: NonEmpty[Seq[UniqueIdentifier]],
      transactionTypes: Set[Code],
      op: TopologyChangeOp = TopologyChangeOp.Replace,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Map[UniqueIdentifier, Seq[GenericStoredTopologyTransaction]]] = MonadUtil
    .sequentialTraverse(uid)(lookupForUid(asOf, asOfInclusive, _, transactionTypes, op))
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
    maxCacheSize: PositiveInt,
    enableConsistencyChecks: Boolean,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyStateLookup
    with NamedLogging
    with FlagCloseable
    with HasCloseContext {

  // maintain last successfully processed timestamp
  private val asOf = new AtomicReference[EffectiveTime](EffectiveTime.MinValue)
  // double ended queue with cached items. oldest items are at the beginning, newest items are at the end
  private val cachedKeys = mutable.Queue[StateKey]()
  private val lock = new Mutex()
  // list of newly fetched states. these states will be appended to the cached queue during the next eviction cycle
  private val fresh = new AtomicReference[List[StateKey]](List.empty)
  // TODO(#29400) availability security improvement: we need to limit the number of topology transactions we
  //   keep in memory for a particular UID. in particular:
  //     only keep a subset of the history in memory
  //     ensure that the set of unique keys is limited (which limits the state size).
  //     the latter is currently not enforced
  // the actual state cache UID => Seq[Tx]
  private val stateCache = new TrieMap[StateKey, CacheItem]()
  // tracking the pending additions and removals such that they can subsequently be flushed to the db
  private val pendingAdd = new AtomicReference[List[MaybeUpdatedTx]](List.empty)
  private val pendingRemove = new AtomicReference[Set[MaybeUpdatedTx]](Set.empty)
  // parallel loading for topology lookups
  private val aggregator = BatchAggregator(
    new BatchAggregator.Processor[StateKey, CacheItem.Loaded]() {
      override def kind: String = "topology-loader"
      override def logger: TracedLogger = TopologyStateWriteThroughCache.this.logger
      override def executeBatch(items: NonEmpty[Seq[Traced[StateKey]]])(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): FutureUnlessShutdown[immutable.Iterable[CacheItem.Loaded]] = {
        val (uids, nss) =
          items.map(_.value).foldLeft((Set.empty[UniqueIdentifier], Set.empty[Namespace])) {
            case ((uids, nss), StateKey.StateUid(uid)) => (uids + uid, nss)
            case ((uids, nss), StateKey.StateNamespace(ns)) => (uids, nss + ns)
          }
        store.fetchAllDescending(uids, nss).map(txs => partitionByStateKey(txs.result)).map {
          case (uids, nss) =>
            items.map(_.value).map {
              case stateKey @ StateKey.StateUid(uid) =>
                val txs = uids.get(uid).map(_.result).getOrElse(Seq.empty)
                Loaded(stateKey, txs)
              case stateKey @ StateKey.StateNamespace(ns) =>
                val txs = nss.get(ns).map(_.result).getOrElse(Seq.empty)
                Loaded(stateKey, txs)
            }
        }
      }
      override def prettyItem: Pretty[StateKey] = implicitly
    },
    aggregatorConfig,
  )

  private def partitionByStateKey(
      resultsDesc: Seq[GenericStoredTopologyTransaction]
  )(implicit errorLoggingContext: ErrorLoggingContext): (
      Map[UniqueIdentifier, GenericStoredTopologyTransactions],
      Map[Namespace, GenericStoredTopologyTransactions],
  ) = {
    val forUids =
      resultsDesc.flatMap(c => c.mapping.maybeUid.map((_, c))).groupBy { case (k, v) => k }.map {
        case (k, v) => (k, StoredTopologyTransactions(v.map(_._2)))
      }
    val forNs = resultsDesc.filter(_.mapping.maybeUid.isEmpty).groupBy(_.mapping.namespace).map {
      case (k, v) => (k, StoredTopologyTransactions(v))
    }

    if (enableConsistencyChecks) {
      def checkOrder[K](items: Map[K, GenericStoredTopologyTransactions]): Unit =
        items.foreach { case (k, v) =>
          v.result.sliding(2).filter(_.sizeIs == 2).foreach { items =>
            ErrorUtil.requireState(
              items(0).validFrom.value >= items(1).validFrom.value,
              "Not sorted correctly\n " + items,
            )
          }
        }
      checkOrder(forUids)
      checkOrder(forNs)
    }
    (forUids, forNs)
  }

  override def synchronizerId: Option[PhysicalSynchronizerId] = store.storeId.forSynchronizer

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
  def evict()(implicit traceContext: TraceContext): Int =
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

      val newItems = fresh.getAndSet(List.empty)
      val oldSize = cachedKeys.size // constant time op
      val newItemsSize = newItems.size
      val numToRemove = oldSize + newItemsSize - maxCacheSize.value
      if (numToRemove > 0) {
        val (excess, kept) = go(todo = numToRemove, index = 0, kept = 0, maxIdx = oldSize)
        logger.info(
          s"Completed topology cache eviction: now=${cachedKeys.size + newItemsSize}, fresh=${newItems.size}, cached=$oldSize, kept=$kept, excess=$excess"
        )
      }
      cachedKeys.appendAll(newItems)
      cachedKeys.size
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
    get(stateKey).map { state =>
      val toStore = StoredTopologyTransaction(
        sequenced,
        validFrom = effective,
        validUntil = Option.when(tx.rejectionReason.isDefined || tx.expireImmediately)(effective),
        transaction = tx.transaction,
        rejectionReason = tx.rejectionReason.map(_.asString300),
      )
      val loggerDebug = TopologyMapping.loggerDebug(tx.transaction.mapping.code)
      loggerDebug(s"Adding $toStore")
      def expire(toExpire: MaybeUpdatedTx): Unit = {
        val before = toExpire.expireAt(effective)
        loggerDebug("Marking transaction as expired:" + toExpire)
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
        state
          .findAtMostOne(
            existing =>
              existing.isActive && existing.stored.transaction.isProposal &&
                existing.stored.transaction.transaction.hash == tx.transaction.transaction.hash,
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
        val existing = state.findAtMostOne(
          existing =>
            existing.isActive && !existing.stored.transaction.isProposal && existing.stored.mapping.uniqueKey == tx.mapping.uniqueKey,
          enableConsistencyChecks,
          "append-archive-existing-tx",
        )
        existing match {
          case None if tx.transaction.serial == PositiveInt.one => () // Nothing to archive
          case None =>
            // This case might happen if somebody added an authorized transaction with serial > 1
            // It is odd but supported
            loggerDebug("No existing transaction to expire for " + tx.transaction)
          case Some(toExpire) =>
            ErrorUtil.requireState(
              toExpire.stored.serial.value <= tx.serial.value,
              s"Mismatched serials when expiring existing transaction $toExpire because of\n   $tx",
            )
            expire(toExpire)
        }
        // expire all proposals for this mapping
        state.entries
          .get()
          .filter(existing =>
            existing.isActive && existing.stored.transaction.isProposal && existing.stored.mapping.uniqueKey == tx.mapping.uniqueKey &&
              existing.stored.serial <= tx.serial // (only proposals up to and including the serial of the accepted tx)
          )
          .foreach(expire)
      }

      // add transaction to state
      val maybeUpdatedTx =
        new MaybeUpdatedTx(stateKey, toStore, initPersisted = false, tx.rejectionReason)
      state.entries.getAndUpdate { lst =>
        (maybeUpdatedTx :: lst)
      }.discard
      pendingAdd.getAndUpdate(maybeUpdatedTx :: _).discard
    }

  }

  /** Fetch the provided uids and namespaces and ensure their data is loaded in memory
    *
    * Crash recovery behaviour: If we resume on a restart or crash, we will find previously
    * processed transactions in the store. Here, we choose the strategy to keep the data in the
    * store, relying on idempotent inserts, but we drop all changes that happened in the future.
    */
  def fetch(
      effective: EffectiveTime,
      uids: Set[UniqueIdentifier],
      namespaces: Set[Namespace],
      storeIsEmpty: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val keys: Set[StateKey] = (uids.map(StateUid.apply) ++ namespaces.map(StateNamespace.apply))
    // if we know that the story is empty, bypass the db lookup
    if (storeIsEmpty) {
      keys.foreach(key => stateCache.putIfAbsent(key, Loaded(key, Seq.empty)).discard)
      fresh.getAndUpdate(keys.toList ++ _).discard
      FutureUnlessShutdown.unit
    } else {
      // TODO (#29400) use runBatch to load them at once
      keys.toList.parTraverse_(get(_).map { loaded =>
        loaded.dropPendingChanges(asOfInclusive = effective.value)
      })
    }
  }

  protected def get(
      key: StateKey
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[CacheItem.Loaded] =
    // We are missing an getAndUpdate here. As we want to avoid creating too many unused
    // promises, we'll use the "get / putIfAbsent" which will only create a potentially unused
    // promise if we really race
    stateCache.get(key) match {
      // we have loaded it already, or we are currently loading it
      case Some(present) => present.value.map(_.readAccess())
      // init loading
      case None =>
        val promise = PromiseUnlessShutdown.unsupervised[CacheItem.Loaded]()
        stateCache.putIfAbsent(key, CacheItem.Loading(promise)) match {
          // if we raced and lost, return the other one
          case Some(other) =>
            promise.completeWithUS(other.value).discard
            other.value.map(_.readAccess())
          // otherwise, start loading
          case None =>
            aggregator.run(key).map { loaded =>
              stateCache.put(key, loaded).discard
              ErrorUtil.requireState(
                promise.outcome(loaded),
                "Load promise was already completed. This should not be possible",
              )
              // mark this as loaded (this will schedule its eviction later on)
              // loading cannot race with eviction as only items which we have added to fresh can be evicted
              // but we only add them to fresh once they are successfully loaded.
              if (enableConsistencyChecks) lock.exclusive {
                val previous = fresh.getAndUpdate(old => key :: old)
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
                fresh.getAndUpdate(old => key :: old).discard
              }
              loaded
            }
        }
    }

  /** Drop everything
    *
    * Not thread safe. If the state processor runs as part of the synchronizer topology manager or
    * authorized topology manager, then we might have to reset the cache if we abort the writes, or
    * if we dispatch to the synchronizer.
    */
  def dropEverything(): Unit =
    lock.exclusive {
      // TODO(#29400) make this more efficient for topology managers by coupling the state processors of the
      //   topology manager and the one of the transaction processor
      //   The challenge is that the synchronizer topology manager operates against a mixed state, given by the state
      //   on the synchronizer plus the local changes. This means that we would have to create a "cache on top of a cache",
      //   where the cache of the topology manager is in turn backed by the cache of the state processor.
      stateCache.clear()
      pendingAdd.set(List.empty)
      pendingRemove.set(Set.empty)
      fresh.set(List.empty)
      cachedKeys.clear()
    }

  override def lookupForUid(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      uid: UniqueIdentifier,
      transactionTypes: Set[Code],
      op: TopologyChangeOp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] =
    get(StateUid(uid)).map(_.filterState(asOf, asOfInclusive, transactionTypes, op))

  override def lookupHistoryForUid(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      uid: UniqueIdentifier,
      transactionType: Code,
      op: TopologyChangeOp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] =
    get(StateUid(uid)).map(_.history(asOf, asOfInclusive, Set(transactionType), op))

  override def lookupForNamespace(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      ns: Namespace,
      transactionTypes: Set[Code],
      op: TopologyChangeOp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericStoredTopologyTransaction]] = get(StateNamespace(ns)).map(
    _.filterState(asOf, asOfInclusive, transactionTypes, op)
  )

  override def lookupForNamespaces(
      asOf: EffectiveTime,
      asOfInclusive: Boolean,
      ns: NonEmpty[Seq[Namespace]],
      transactionTypes: Set[Code],
      op: TopologyChangeOp = TopologyChangeOp.Replace,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Map[Namespace, Seq[GenericStoredTopologyTransaction]]] =
    MonadUtil
      .sequentialTraverse(ns)(lookupForNamespace(asOf, asOfInclusive, _, transactionTypes, op))
      .map(seq => ns.toSeq.zip(seq).toMap)

  /** Find the current transaction active for the given unique key
    *
    * Used by the [[com.digitalasset.canton.topology.TopologyStateProcessor]] to get the current
    * inStore transaction.
    */
  def lookupActiveForMapping(
      mapping: TopologyMapping
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] =
    get(StateKey(mapping)).map { loaded =>
      loaded
        .findAtMostOne(
          { tx =>
            val stored = tx.stored
            stored.rejectionReason.isEmpty && stored.mapping.code == mapping.code && tx.isActive && !stored.transaction.isProposal && stored.mapping.uniqueKey == mapping.uniqueKey
          },
          enableConsistencyChecks,
          s"lookup-active-for-mapping $mapping",
        )
        .map(_.stored)
    }

  /** Lookup the pending proposal for the given transaction
    *
    * Used by [[com.digitalasset.canton.topology.TopologyStateProcessor]] to check for any pending
    * proposal for the given transaction
    */
  def lookupPendingProposal(
      tx: TopologyTransaction[TopologyChangeOp, TopologyMapping]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] =
    get(StateKey(tx.mapping)).map { loaded =>
      loaded
        .findAtMostOne(
          { current =>
            val stored = current.stored
            stored.rejectionReason.isEmpty && current.isActive && stored.transaction.isProposal && tx.hash == stored.transaction.transaction.hash
          },
          enableConsistencyChecks,
          s"lookup-pending-proposal for $tx",
        )
        .map(_.stored)
    }
}

object TopologyStateWriteThroughCache {

  private[cache] sealed trait StateKey extends PrettyPrinting
  private[cache] object StateKey {
    final case class StateUid(uid: UniqueIdentifier) extends StateKey {
      override protected def pretty: Pretty[StateUid] = prettyOfString(_.uid.toString)
    }
    final case class StateNamespace(namespace: Namespace) extends StateKey {
      override protected def pretty: Pretty[StateNamespace] = prettyOfString(_.namespace.toString)
    }
    def apply(mapping: TopologyMapping): StateKey =
      mapping.maybeUid match {
        case None => StateNamespace(mapping.namespace)
        case Some(uid) => StateUid(uid)
      }
  }

  /** Trait to express a cached item which may be loaded or still loading */
  private[cache] sealed trait CacheItem {
    def value: FutureUnlessShutdown[Loaded]
  }
  private[cache] object CacheItem {
    final case class Loading(promise: PromiseUnlessShutdown[Loaded]) extends CacheItem {
      override def value: FutureUnlessShutdown[Loaded] = promise.futureUS
    }
    final case class Loaded(stateKey: StateKey, initial: Seq[GenericStoredTopologyTransaction])
        extends CacheItem {
      override def value: FutureUnlessShutdown[Loaded] = FutureUnlessShutdown.pure(this)

      def readAccess(): Loaded = {
        accessed.set(true)
        this
      }
      // track whether this has been recently accessed (via get())
      val accessed: AtomicBoolean = new AtomicBoolean(true)
      // the list of transactions ordered desc so that we'll likely find the
      // relevant at or close to head.
      // reading while writing is fine as reading new data will only happen once asOf has been updated
      val entries: AtomicReference[List[MaybeUpdatedTx]] = new AtomicReference(
        initial.toList.map(stored =>
          new MaybeUpdatedTx(stateKey, stored, initPersisted = true, None)
        )
      )

      /** find the transaction matching the predicate
        *
        * most read operations should find exactly one transaction. as an additional check, we can
        * also scan the entire state to ensure that there are not multiple that match by accident.
        */
      def findAtMostOne(
          filter: MaybeUpdatedTx => Boolean,
          checkConsistency: Boolean,
          context: => String,
      )(implicit errorLoggingContext: ErrorLoggingContext): Option[MaybeUpdatedTx] = if (
        checkConsistency
      ) {
        val items = entries.get().filter(filter)
        ErrorUtil.requireState(
          items.sizeIs <= 1,
          s"Expected at most one for $context, found ${items.size}: ${items.mkString("\n  ")}",
        )
        items.headOption
      } else {
        entries.get().find(filter)
      }

      /** returns the history of the transactions (excluding proposals) up to the timestamp matching
        * the predicate
        */
      def history(
          asOf: EffectiveTime,
          asOfInclusive: Boolean,
          transactionTypes: Set[Code],
          op: TopologyChangeOp,
      ): Seq[GenericStoredTopologyTransaction] = entries
        .get()
        .filter(tx =>
          tx.isEffectiveAtOrBefore(asOf, asOfInclusive) && transactionTypes.contains(
            tx.signed.mapping.code
          ) && !tx.signed.isProposal && op == tx.signed.operation
        )
        .map(_.stored)

      def filterState(
          asOf: EffectiveTime,
          asOfInclusive: Boolean,
          transactionTypes: Set[Code],
          op: TopologyChangeOp,
      ): Seq[GenericStoredTopologyTransaction] =
        entries
          .get()
          .filter(tx =>
            tx.isEffectiveAt(asOf, asOfInclusive) && transactionTypes.contains(
              tx.signed.transaction.mapping.code
            ) && !tx.signed.isProposal && op == tx.signed.transaction.operation
          )
          .map(_.stored)

      def dropPendingChanges(asOfInclusive: CantonTimestamp): Unit =
        entries
          .getAndUpdate(
            // remove all future additions changes
            _.filter(_.stored.validFrom.value < asOfInclusive)
              .map { tx =>
                // remove any deactivation in the future
                tx.dropPendingChange(asOfInclusive = asOfInclusive)
                tx
              }
          )
          .discard

    }
  }

  /** A cached but mutable topology item
    *
    * @param initStored
    *   the initial value of the stored transaction
    * @param initPersisted
    *   if true then the stored transaction was loaded from db, otherwise it was added by processing
    */
  private[cache] final class MaybeUpdatedTx(
      val stateKey: StateKey,
      initStored: GenericStoredTopologyTransaction,
      initPersisted: Boolean,
      initRejectionReason: Option[TopologyTransactionRejection], // only used during initial storing
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

    /** is not rejected and effective at */
    def isEffectiveAt(asOf: EffectiveTime, asOfInclusive: Boolean): Boolean = {
      val tx = current.get()
      (if (asOfInclusive) {
         tx.validFrom.value <= asOf.value && tx.validUntil.forall(ts => ts.value > asOf.value)
       } else {
         tx.validFrom.value < asOf.value && tx.validUntil.forall(ts => ts.value >= asOf.value)
       }) && tx.rejectionReason.isEmpty
    }

    /** is not rejected and effective at or before the given timestamp */
    def isEffectiveAtOrBefore(asOf: EffectiveTime, asOfInclusive: Boolean): Boolean = {
      val tx = current.get()
      (if (asOfInclusive) {
         tx.validFrom.value <= asOf.value
       } else {
         tx.validFrom.value < asOf.value
       }) && tx.rejectionReason.isEmpty
    }

    def stored: GenericStoredTopologyTransaction = current.get()

    def toValidated: GenericValidatedTopologyTransaction = ValidatedTopologyTransaction(
      stored.transaction,
      initRejectionReason,
      expireImmediately = stored.validUntil.nonEmpty,
    )

    override protected def pretty: Pretty[MaybeUpdatedTx] = prettyOfClass(
      param("stored", _.stored),
      paramIfTrue("persisted", _.persisted.get()),
    )

    /** Removes any future deactivation (used to drop pending changes upon replay) */
    def dropPendingChange(asOfInclusive: CantonTimestamp): Unit =
      current.getAndUpdate { cur =>
        if (cur.validUntil.exists(ef => ef.value >= asOfInclusive))
          cur.copy(validUntil = None)
        else cur
      }.discard
  }

}
