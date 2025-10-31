// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.CantonRequireTypes.{String185, String300}
import com.digitalasset.canton.config.{CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbIndexedStringStore
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.topology.store.TopologyStoreId
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.{PositionedParameters, SetParameter}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

trait IndexedString[E] {
  def item: E
  def index: Int
}
object IndexedString {

  abstract class Impl[E](val item: E) extends IndexedString[E]

  implicit val setParameterIndexedString: SetParameter[IndexedString[?]] =
    (d: IndexedString[?], pp: PositionedParameters) => pp.setInt(d.index)

  implicit val setParameterIndexedStringO: SetParameter[Option[IndexedString[?]]] =
    (d: Option[IndexedString[?]], pp: PositionedParameters) => pp.setIntOption(d.map(_.index))

}

// common interface for companion objects
abstract class IndexedStringFromDb[A <: IndexedString[B], B] {

  protected def buildIndexed(item: B, index: Int): A
  protected def asString(item: B): String300
  protected def dbTyp: IndexedStringType
  protected def fromString(str: String300, index: Int): Either[String, A]

  def indexed(
      indexedStringStore: IndexedStringStore
  )(item: B)(implicit ec: ExecutionContext, traceContext: TraceContext): FutureUnlessShutdown[A] =
    indexedStringStore
      .getOrCreateIndex(dbTyp, asString(item))
      .map(buildIndexed(item, _))

  def fromDbIndexOT(context: String, indexedStringStore: IndexedStringStore)(
      index: Int
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): OptionT[FutureUnlessShutdown, A] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext

    fromDbIndexET(indexedStringStore)(index).leftMap { err =>
      loggingContext.logger.error(
        s"Corrupt log id: $index for $dbTyp within context $context: $err"
      )(loggingContext.traceContext)
    }.toOption
  }

  def fromDbIndexET(
      indexedStringStore: IndexedStringStore
  )(index: Int)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, A] =
    EitherT(indexedStringStore.getForIndex(dbTyp, index).map { strO =>
      for {
        str <- strO.toRight("No entry for given index")
        parsed <- fromString(str, index)
      } yield parsed
    })
}

final case class IndexedSynchronizer private (synchronizerId: SynchronizerId, index: Int)
    extends IndexedString.Impl[SynchronizerId](synchronizerId) {
  require(
    index > 0,
    s"Illegal index $index. The index must be positive to prevent clashes with participant event log ids.",
  )
}

object IndexedSynchronizer extends IndexedStringFromDb[IndexedSynchronizer, SynchronizerId] {

  /** @throws java.lang.IllegalArgumentException
    *   if `index <= 0`.
    */
  @VisibleForTesting
  def tryCreate(synchronizerId: SynchronizerId, index: Int): IndexedSynchronizer =
    IndexedSynchronizer(synchronizerId, index)

  override protected def dbTyp: IndexedStringType = IndexedStringType.synchronizerId

  override protected def buildIndexed(item: SynchronizerId, index: Int): IndexedSynchronizer =
    // save, because buildIndexed is only called with indices created by IndexedStringStores.
    // These indices are positive by construction.
    checked(tryCreate(item, index))

  override protected def asString(item: SynchronizerId): String300 =
    item.toLengthLimitedString.asString300

  override protected def fromString(
      str: String300,
      index: Int,
  ): Either[String, IndexedSynchronizer] =
    // save, because fromString is only called with indices created by IndexedStringStores.
    // These indices are positive by construction.
    SynchronizerId.fromString(str.unwrap).map(checked(tryCreate(_, index)))
}

final case class IndexedPhysicalSynchronizer private (
    synchronizerId: PhysicalSynchronizerId,
    index: Int,
) extends IndexedString.Impl[PhysicalSynchronizerId](synchronizerId) {
  require(
    index > 0,
    s"Illegal index $index. The index must be positive to prevent clashes with participant event log ids.",
  )
}

object IndexedPhysicalSynchronizer
    extends IndexedStringFromDb[IndexedPhysicalSynchronizer, PhysicalSynchronizerId] {

  /** @throws java.lang.IllegalArgumentException
    *   if `index <= 0`.
    */
  @VisibleForTesting
  def tryCreate(
      synchronizerId: PhysicalSynchronizerId,
      index: Int,
  ): IndexedPhysicalSynchronizer =
    IndexedPhysicalSynchronizer(synchronizerId, index)

  override protected def dbTyp: IndexedStringType = IndexedStringType.physicalSynchronizerId

  override protected def buildIndexed(
      item: PhysicalSynchronizerId,
      index: Int,
  ): IndexedPhysicalSynchronizer =
    // save, because buildIndexed is only called with indices created by IndexedStringStores.
    // These indices are positive by construction.
    checked(tryCreate(item, index))

  override protected def asString(item: PhysicalSynchronizerId): String300 =
    item.toLengthLimitedString

  override protected def fromString(
      str: String300,
      index: Int,
  ): Either[String, IndexedPhysicalSynchronizer] =
    // save, because fromString is only called with indices created by IndexedStringStores.
    // These indices are positive by construction.
    PhysicalSynchronizerId.fromString(str.unwrap).map(checked(tryCreate(_, index)))
}

final case class IndexedTopologyStoreId private (
    topologyStoreId: TopologyStoreId,
    index: Int,
) extends IndexedString.Impl[TopologyStoreId](topologyStoreId)

object IndexedTopologyStoreId extends IndexedStringFromDb[IndexedTopologyStoreId, TopologyStoreId] {

  private val tempStoreMarker = "temp"
  private val tempPrefix = tempStoreMarker + UniqueIdentifier.delimiter
  private val tempSuffix = UniqueIdentifier.delimiter + tempStoreMarker

  @VisibleForTesting
  def tryCreate(
      topologyStoreId: TopologyStoreId,
      index: Int,
  ): IndexedTopologyStoreId =
    IndexedTopologyStoreId(topologyStoreId, index)

  override protected def buildIndexed(item: TopologyStoreId, index: Int): IndexedTopologyStoreId =
    IndexedTopologyStoreId(item, index)

  override protected def asString(item: TopologyStoreId): String300 = item match {
    case TopologyStoreId.SynchronizerStore(psid) => psid.toLengthLimitedString
    case TopologyStoreId.AuthorizedStore => checked(String300.tryCreate("Authorized"))
    case TopologyStoreId.TemporaryStore(name) =>
      checked(
        String300.tryCreate(
          tempPrefix + name.str + tempSuffix
        )
      )
  }

  override protected def dbTyp: IndexedStringType = IndexedStringType.topologyStoreId

  override protected def fromString(
      str: String300,
      index: Int,
  ): Either[String, IndexedTopologyStoreId] = if (str.str == "Authorized")
    Right(IndexedTopologyStoreId(TopologyStoreId.AuthorizedStore, index))
  else if (str.str.startsWith(tempPrefix) && str.str.endsWith(tempSuffix)) {
    Right(
      IndexedTopologyStoreId(
        TopologyStoreId.TemporaryStore(
          checked(
            String185.tryCreate(
              str.str.substring(tempPrefix.length, str.str.length - tempSuffix.length)
            )
          )
        ),
        index,
      )
    )
  } else {
    PhysicalSynchronizerId.fromString(str.str).map { psid =>
      IndexedTopologyStoreId(
        TopologyStoreId.SynchronizerStore(psid),
        index,
      )
    }
  }
}

final case class IndexedStringType private (source: Int, description: String)
object IndexedStringType {

  private val ids: mutable.Map[Int, IndexedStringType] =
    mutable.TreeMap.empty[Int, IndexedStringType]

  /** Creates a new [[IndexedStringType]] with a given description */
  def apply(source: Int, description: String): IndexedStringType = {
    val item = new IndexedStringType(source, description)
    ids.put(source, item).foreach { oldItem =>
      throw new IllegalArgumentException(
        s"requirement failed: IndexedStringType with id=$source already exists as $oldItem"
      )
    }
    item
  }

  val synchronizerId: IndexedStringType = IndexedStringType(1, "synchronizerId")
  val physicalSynchronizerId: IndexedStringType = IndexedStringType(2, "physicalSynchronizerId")
  val topologyStoreId: IndexedStringType = IndexedStringType(3, "topologyStoreId")
}

/** uid index such that we can store integers instead of long strings in our database */
trait IndexedStringStore extends AutoCloseable {
  def getOrCreateIndex(dbTyp: IndexedStringType, str: String300)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int]
  def getForIndex(dbTyp: IndexedStringType, idx: Int)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[String300]]
}

object IndexedStringStore {
  def create(
      storage: Storage,
      config: CacheConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): IndexedStringStore =
    storage match {
      case _: MemoryStorage => InMemoryIndexedStringStore()
      case jdbc: DbStorage =>
        new IndexedStringCache(
          new DbIndexedStringStore(jdbc, timeouts, loggerFactory),
          config,
          loggerFactory,
        )
    }
}

class IndexedStringCache(
    parent: IndexedStringStore,
    config: CacheConfig,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends IndexedStringStore
    with NamedLogging {

  private val str2Index
      : TracedAsyncLoadingCache[FutureUnlessShutdown, (String300, IndexedStringType), Int] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, (String300, IndexedStringType), Int](
      cache = config.buildScaffeine(),
      loader = implicit tc => { case (str, typ) =>
        parent
          .getOrCreateIndex(typ, str)
          .map { idx =>
            index2strFUS.put((idx, typ), Some(str))
            idx
          }
      },
    )(logger, "str2Index")

  // (index,typ)
  private val index2strFUS
      : TracedAsyncLoadingCache[FutureUnlessShutdown, (Int, IndexedStringType), Option[String300]] =
    ScaffeineCache
      .buildTracedAsync[FutureUnlessShutdown, (Int, IndexedStringType), Option[String300]](
        cache = config.buildScaffeine(),
        loader = implicit tc => { case (idx, typ) =>
          parent
            .getForIndex(typ, idx)
            .map {
              case Some(str) =>
                str2Index.put((str, typ), idx)
                Some(str)
              case None => None
            }
        },
      )(logger, "index2str")

  override def getForIndex(
      dbTyp: IndexedStringType,
      idx: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[String300]] =
    index2strFUS.get((idx, dbTyp))

  override def getOrCreateIndex(
      dbTyp: IndexedStringType,
      str: String300,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    str2Index.get((str, dbTyp))

  override def close(): Unit = {
    str2Index.invalidateAll()
    str2Index.cleanUp()
    index2strFUS.invalidateAll()
    index2strFUS.cleanUp()
    parent.close()
  }
}
