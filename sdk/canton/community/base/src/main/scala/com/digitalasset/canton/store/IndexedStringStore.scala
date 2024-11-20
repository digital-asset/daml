// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.{CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbIndexedStringStore
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.topology.DomainId
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

  implicit val setParameterIndexedString: SetParameter[IndexedString[_]] =
    (d: IndexedString[_], pp: PositionedParameters) => pp.setInt(d.index)

  implicit val setParameterIndexedStringO: SetParameter[Option[IndexedString[_]]] =
    (d: Option[IndexedString[_]], pp: PositionedParameters) => pp.setIntOption(d.map(_.index))

}

// common interface for companion objects
abstract class IndexedStringFromDb[A <: IndexedString[B], B] {

  protected def buildIndexed(item: B, index: Int): A
  protected def asString(item: B): String300
  protected def dbTyp: IndexedStringType
  protected def fromString(str: String300, index: Int): Either[String, A]

  def indexed(
      indexedStringStore: IndexedStringStore
  )(item: B)(implicit ec: ExecutionContext): FutureUnlessShutdown[A] =
    indexedStringStore
      .getOrCreateIndex(dbTyp, asString(item))
      .map(buildIndexed(item, _))

  def fromDbIndexOT(context: String, indexedStringStore: IndexedStringStore)(
      index: Int
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): OptionT[FutureUnlessShutdown, A] =
    fromDbIndexET(indexedStringStore)(index).leftMap { err =>
      loggingContext.logger.error(
        s"Corrupt log id: $index for $dbTyp within context $context: $err"
      )(loggingContext.traceContext)
    }.toOption

  def fromDbIndexET(
      indexedStringStore: IndexedStringStore
  )(index: Int)(implicit ec: ExecutionContext): EitherT[FutureUnlessShutdown, String, A] =
    EitherT(indexedStringStore.getForIndex(dbTyp, index).map { strO =>
      for {
        str <- strO.toRight("No entry for given index")
        parsed <- fromString(str, index)
      } yield parsed
    })
}

final case class IndexedDomain private (domainId: DomainId, index: Int)
    extends IndexedString.Impl[DomainId](domainId) {
  require(
    index > 0,
    s"Illegal index $index. The index must be positive to prevent clashes with participant event log ids.",
  )
}

object IndexedDomain extends IndexedStringFromDb[IndexedDomain, DomainId] {

  /** @throws java.lang.IllegalArgumentException if `index <= 0`.
    */
  @VisibleForTesting
  def tryCreate(domainId: DomainId, index: Int): IndexedDomain =
    IndexedDomain(domainId, index)

  override protected def dbTyp: IndexedStringType = IndexedStringType.domainId

  override protected def buildIndexed(item: DomainId, index: Int): IndexedDomain =
    // save, because buildIndexed is only called with indices created by IndexedStringStores.
    // These indices are positive by construction.
    checked(tryCreate(item, index))

  override protected def asString(item: DomainId): String300 =
    item.toLengthLimitedString.asString300

  override protected def fromString(str: String300, index: Int): Either[String, IndexedDomain] =
    // save, because fromString is only called with indices created by IndexedStringStores.
    // These indices are positive by construction.
    DomainId.fromString(str.unwrap).map(checked(tryCreate(_, index)))
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

  val domainId: IndexedStringType = IndexedStringType(1, "domainId")

}

/** uid index such that we can store integers instead of long strings in our database */
trait IndexedStringStore extends AutoCloseable {
  def getOrCreateIndex(dbTyp: IndexedStringType, str: String300): FutureUnlessShutdown[Int]
  def getForIndex(dbTyp: IndexedStringType, idx: Int): FutureUnlessShutdown[Option[String300]]
}

object IndexedStringStore {
  def create(
      storage: Storage,
      config: CacheConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): IndexedStringStore =
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
)(implicit ec: ExecutionContext, tc: TraceContext)
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
    )(logger)

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
      )(logger)

  override def getForIndex(
      dbTyp: IndexedStringType,
      idx: Int,
  ): FutureUnlessShutdown[Option[String300]] =
    index2strFUS.get((idx, dbTyp))

  override def getOrCreateIndex(
      dbTyp: IndexedStringType,
      str: String300,
  ): FutureUnlessShutdown[Int] =
    str2Index.get((str, dbTyp))

  override def close(): Unit = parent.close()
}
