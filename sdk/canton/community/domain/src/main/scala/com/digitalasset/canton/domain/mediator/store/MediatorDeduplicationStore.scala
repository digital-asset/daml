// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.mediator.store.MediatorDeduplicationStore.DeduplicationData
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.resource.{DbStorage, DbStore, MemoryStorage, Storage}
import com.digitalasset.canton.topology.{MediatorId, Member}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil}
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.{GetResult, SetParameter}

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

private[mediator] trait MediatorDeduplicationStore extends NamedLogging with FlagCloseable {

  /** Stores deduplication data for a given uuid.
    */
  private val dataByUuid: TrieMap[UUID, NonEmpty[Set[DeduplicationData]]] = TrieMap()

  /** Stores deduplication data by expiration time (for efficient pruning). */
  private val uuidByExpiration: ConcurrentNavigableMap[CantonTimestamp, NonEmpty[Set[UUID]]] =
    new ConcurrentSkipListMap()

  protected val initialized: AtomicBoolean = new AtomicBoolean()

  /** Clients must call this method before any other method.
    *
    * @param firstEventTs the timestamp used to subscribe to the sequencer, i.e.,
    *                     all data with a requestTime greater than or equal to `firstEventTs` will be deleted so that
    *                     sequencer events can be replayed
    */
  def initialize(firstEventTs: CantonTimestamp)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit] = {
    require(!initialized.getAndSet(true), "The store most not be initialized more than once!")
    doInitialize(firstEventTs)
  }

  /** Populate in-memory caches and
    * delete all data with `requestTime` greater than or equal to `deleteFromInclusive`.
    */
  protected def doInitialize(deleteFromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit]

  private def requireInitialized()(implicit traceContext: TraceContext): Unit =
    ErrorUtil.requireState(initialized.get(), "The initialize method needs to be called first.")

  /** Yields the data stored for a given `uuid` that is not expired at `timestamp`.
    */
  def findUuid(uuid: UUID, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Set[DeduplicationData] = {
    requireInitialized()
    dataByUuid
      .get(uuid)
      .fold(Set.empty[DeduplicationData])(_.forgetNE)
      .filter(_.expireAfter >= timestamp)
  }

  @VisibleForTesting
  def allData()(implicit traceContext: TraceContext): Set[DeduplicationData] = {
    requireInitialized()
    dataByUuid.values.toSet.flatten
  }

  /** See the documentation of the other `store` method.
    */
  def store(
      data: DeduplicationData
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit] = {
    requireInitialized()

    // Updating in-memory state synchronously, so that changes are effective when the method returns,
    // as promised in the scaladoc.
    storeInMemory(data)

    persist(data)
  }

  protected def storeInMemory(data: DeduplicationData): Unit = {
    dataByUuid
      .updateWith(data.uuid) {
        case None => Some(NonEmpty(Set, data))
        case Some(existing) => Some(existing.incl(data))
      }
      .discard

    // The map uuidByExpiration is updated second, so that a concurrent call to prune
    // won't leave behind orphaned data in dataByUuid.
    uuidByExpiration.asScala
      .updateWith(data.expireAfter) {
        case None => Some(NonEmpty(Set, data.uuid))
        case Some(existing) => Some(existing.incl(data.uuid))
      }
      .discard[Option[NonEmpty[Set[UUID]]]]
  }

  /** Persist data to the database. */
  protected def persist(
      data: DeduplicationData
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit]

  /** Stores the given `uuid` together with `requestTime` and `expireAfter`.
    *
    * This method supports concurrent invocations.
    * Changes are effective as soon as this method returns.
    * If the store supports persistence, changes are persistent as soon as the returned future completes.
    */
  def store(uuid: UUID, requestTime: CantonTimestamp, expireAfter: CantonTimestamp)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit] = store(DeduplicationData(uuid, requestTime, expireAfter))

  /** Delete all data with `expireAt` before than or equal to `upToInclusive`.
    *
    * If some data is concurrently stored and pruned, some data may remain in the in-memory caches and / or in the database.
    * Such data will be deleted by a subsequent call to `prune`.
    */
  def prune(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit] = {
    requireInitialized()

    // Take a defensive copy so that we can safely iterate over it.
    val uuidByExpirationToPrune =
      uuidByExpiration.subMap(CantonTimestamp.MinValue, true, upToInclusive, true).asScala.to(Map)

    for ((expireAt, uuids) <- uuidByExpirationToPrune) {
      // Delete this first so that a concurrent call to `store` does not leave behind orphaned data.
      // Delete this only if `uuids` hasn't changed concurrently.
      uuidByExpiration.remove(expireAt, uuids)

      for (uuid <- uuids) {
        dataByUuid
          .updateWith(uuid) {
            case None => None
            case Some(existing) => NonEmpty.from(existing.filter(_.expireAfter > upToInclusive))
          }
          .discard[Option[NonEmpty[Set[DeduplicationData]]]]
      }
    }

    prunePersistentData(upToInclusive)
  }

  /** Delete all persistent data with `expireAt` before than or equal to `upToInclusive`.
    */
  protected def prunePersistentData(
      upToInclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit]
}

private[mediator] object MediatorDeduplicationStore {
  def apply(
      mediatorId: MediatorId,
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      batchAggregatorConfig: BatchAggregatorConfig = BatchAggregatorConfig(),
  )(implicit executionContext: ExecutionContext): MediatorDeduplicationStore = storage match {
    case _: MemoryStorage => new InMemoryMediatorDeduplicationStore(loggerFactory, timeouts)
    case dbStorage: DbStorage =>
      new DbMediatorDeduplicationStore(
        mediatorId,
        dbStorage,
        timeouts,
        batchAggregatorConfig,
        loggerFactory,
      )
  }

  final case class DeduplicationData(
      uuid: UUID,
      requestTime: CantonTimestamp,
      expireAfter: CantonTimestamp,
  ) extends PrettyPrinting {
    override def pretty: Pretty[DeduplicationData] =
      prettyOfClass(
        param("uuid", _.uuid),
        param("requestTime", _.requestTime),
        param("expireAfter", _.expireAfter),
      )
  }

  import DbStorage.Implicits.*

  implicit val setParameterDeduplicationData: SetParameter[DeduplicationData] = {
    case (DeduplicationData(uuid, requestTime, expireAfter), pp) =>
      pp >> uuid
      pp >> requestTime
      pp >> expireAfter
  }

  implicit val getResultDeduplicationData: GetResult[DeduplicationData] = GetResult { r =>
    val uuid = r.<<[UUID]
    val requestTime = r.<<[CantonTimestamp]
    val expireAfter = r.<<[CantonTimestamp]
    DeduplicationData(uuid, requestTime, expireAfter)
  }
}

private[mediator] class InMemoryMediatorDeduplicationStore(
    override protected val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends MediatorDeduplicationStore
    with NamedLogging {

  override protected def doInitialize(deleteFromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit] = Future.unit

  override protected def persist(data: DeduplicationData)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit] = Future.unit

  override protected def prunePersistentData(
      upToInclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit] = Future.unit
}

// Note: this class ignores the inherited close context from the DbStore trait and instead relies purely on
// callerCloseContexts parameters in each public method. This is an attempt to implement a different way to manage close
// context propagation to improve and simplify shutdown semantics and reliability. For that reason closing this store
// is inconsequential. DB operations won't be retried if and only if the caller's close context is closing.
private[mediator] class DbMediatorDeduplicationStore(
    mediatorId: MediatorId,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    batchAggregatorConfig: BatchAggregatorConfig,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends MediatorDeduplicationStore
    with DbStore {

  import Member.DbStorageImplicits.*
  import storage.api.*

  override protected def doInitialize(
      deleteFromInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit] = {
    for {
      _ <- storage.update_(
        sqlu"""delete from mediator_deduplication_store
              where mediator_id = $mediatorId and request_time >= $deleteFromInclusive""",
        functionFullName,
      )(traceContext, callerCloseContext)

      activeUuids <- storage.query(
        sql"""select uuid, request_time, expire_after from mediator_deduplication_store
             where mediator_id = $mediatorId and expire_after > $deleteFromInclusive"""
          .as[DeduplicationData],
        functionFullName,
      )(traceContext, callerCloseContext)
    } yield {
      activeUuids.foreach(storeInMemory)
    }
  }

  override protected def persist(data: DeduplicationData)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit] =
    batchAggregator.run(data)(executionContext, traceContext, callerCloseContext)

  private val batchAggregator = {
    val processor: BatchAggregator.Processor[DeduplicationData, Unit] =
      new BatchAggregator.Processor[DeduplicationData, Unit] {
        override val kind: String = "deduplication data"

        override def logger: TracedLogger = DbMediatorDeduplicationStore.this.logger

        override def executeBatch(items: NonEmpty[Seq[Traced[DeduplicationData]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): Future[Seq[Unit]] = {
          // The query does not have to be idempotent, because the stores don't have unique indices and
          // the data gets deduplicated on the read path.
          val action = DbStorage.bulkOperation_(
            """insert into mediator_deduplication_store(mediator_id, uuid, request_time, expire_after)
              |values (?, ?, ?, ?)""".stripMargin,
            items,
            storage.profile,
          ) { pp => data =>
            pp >> mediatorId
            pp >> data.value
          }

          for {
            _ <- storage.queryAndUpdate(action, functionFullName)(traceContext, callerCloseContext)
          } yield Seq.fill(items.size)(())
        }

        override def prettyItem: Pretty[DeduplicationData] = implicitly
      }

    BatchAggregator(
      processor,
      batchAggregatorConfig,
    )
  }

  override protected def prunePersistentData(
      upToInclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit] =
    storage.update_(
      sqlu"""delete from mediator_deduplication_store
          where mediator_id = $mediatorId and expire_after <= $upToInclusive""",
      functionFullName,
    )(traceContext, callerCloseContext)
}
