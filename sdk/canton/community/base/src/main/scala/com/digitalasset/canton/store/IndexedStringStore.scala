// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.{CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbIndexedStringStore
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.topology.DomainId
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.{PositionedParameters, SetParameter}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

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
  )(item: B)(implicit ec: ExecutionContext): Future[A] =
    indexedStringStore
      .getOrCreateIndex(dbTyp, asString(item))
      .map(buildIndexed(item, _))

  def fromDbIndexOT(context: String, indexedStringStore: IndexedStringStore)(
      index: Int
  )(implicit ec: ExecutionContext, loggingContext: ErrorLoggingContext): OptionT[Future, A] = {
    fromDbIndexET(indexedStringStore)(index).leftMap { err =>
      loggingContext.logger.error(
        s"Corrupt log id: ${index} for ${dbTyp} within context $context: $err"
      )(loggingContext.traceContext)
    }.toOption
  }

  def fromDbIndexET(
      indexedStringStore: IndexedStringStore
  )(index: Int)(implicit ec: ExecutionContext): EitherT[Future, String, A] = {
    EitherT(indexedStringStore.getForIndex(dbTyp, index).map { strO =>
      for {
        str <- strO.toRight("No entry for given index")
        parsed <- fromString(str, index)
      } yield parsed
    })
  }
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

  override protected def buildIndexed(item: DomainId, index: Int): IndexedDomain = {
    // save, because buildIndexed is only called with indices created by IndexedStringStores.
    // These indices are positive by construction.
    checked(tryCreate(item, index))
  }

  override protected def asString(item: DomainId): String300 =
    item.toLengthLimitedString.asString300

  override protected def fromString(str: String300, index: Int): Either[String, IndexedDomain] = {
    // save, because fromString is only called with indices created by IndexedStringStores.
    // These indices are positive by construction.
    DomainId.fromString(str.unwrap).map(checked(tryCreate(_, index)))
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

  val domainId: IndexedStringType = IndexedStringType(1, "domainId")
  val memberId: IndexedStringType = IndexedStringType(2, "memberId")

}

/** uid index such that we can store integers instead of long strings in our database */
trait IndexedStringStore extends AutoCloseable {

  def getOrCreateIndex(dbTyp: IndexedStringType, str: String300): Future[Int]
  def getForIndex(dbTyp: IndexedStringType, idx: Int): Future[Option[String300]]

}

object IndexedStringStore {
  def create(
      storage: Storage,
      config: CacheConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
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
)(implicit ec: ExecutionContext)
    extends IndexedStringStore
    with NamedLogging {

  private val str2Index: AsyncLoadingCache[(String300, IndexedStringType), Int] = Scaffeine()
    .maximumSize(config.maximumSize.value)
    .expireAfterAccess(config.expireAfterAccess.underlying)
    .buildAsyncFuture[(String300, IndexedStringType), Int] { case (str, typ) =>
      parent.getOrCreateIndex(typ, str).map { idx =>
        index2str.put((idx, typ), Future.successful(Some(str)))
        idx
      }
    }

  // (index,typ)
  private val index2str: AsyncLoadingCache[(Int, IndexedStringType), Option[String300]] =
    Scaffeine()
      .maximumSize(config.maximumSize.value)
      .expireAfterAccess(config.expireAfterAccess.underlying)
      .buildAsyncFuture[(Int, IndexedStringType), Option[String300]] { case (idx, typ) =>
        parent.getForIndex(typ, idx).map {
          case Some(str) =>
            str2Index.put((str, typ), Future.successful(idx))
            Some(str)
          case None => None
        }
      }

  override def getForIndex(dbTyp: IndexedStringType, idx: Int): Future[Option[String300]] =
    index2str.get((idx, dbTyp))

  override def getOrCreateIndex(dbTyp: IndexedStringType, str: String300): Future[Int] =
    str2Index.get((str, dbTyp))

  override def close(): Unit = parent.close()
}
