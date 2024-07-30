// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, DbStore, MemoryStorage, Storage}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.TransactionIsolation.Serializable

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Store where we keep the core identity of the node
  *
  * In Canton, everybody is known by his unique identifier which consists of a string and a fingerprint of a signing key.
  * Participant nodes and domains are known by their UID. This store here stores the identity of the node.
  */
trait InitializationStore extends AutoCloseable {

  def uid(implicit traceContext: TraceContext): Future[Option[UniqueIdentifier]]

  def setUid(id: UniqueIdentifier)(implicit traceContext: TraceContext): Future[Unit]

  /** Function used for testing dev version flag. */
  @VisibleForTesting
  def throwIfNotDev(implicit traceContext: TraceContext): Future[Boolean]

}

object InitializationStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext
  ): InitializationStore =
    storage match {
      case _: MemoryStorage => new InMemoryInitializationStore(loggerFactory)
      case jdbc: DbStorage => new DbInitializationStore(jdbc, timeouts, loggerFactory)
    }
}

class InMemoryInitializationStore(override protected val loggerFactory: NamedLoggerFactory)
    extends InitializationStore
    with NamedLogging {
  private val myId = new AtomicReference[Option[UniqueIdentifier]](None)
  override def uid(implicit traceContext: TraceContext): Future[Option[UniqueIdentifier]] =
    Future.successful(myId.get())

  override def setUid(id: UniqueIdentifier)(implicit traceContext: TraceContext): Future[Unit] =
    if (myId.compareAndSet(None, Some(id))) Future.successful(())
    // once we get to this branch, we know that the id is already set (so the logic used here is safe)
    else
      ErrorUtil.requireArgumentAsync(
        myId.get().contains(id),
        s"Unique id of node is already defined as ${myId.get().map(_.toString).getOrElse("")} and can't be changed to $id!",
      )

  override def close(): Unit = ()

  override def throwIfNotDev(implicit traceContext: TraceContext): Future[Boolean] =
    Future.failed(new NotImplementedError("isDev does not make sense on the in-memory store"))

}

class DbInitializationStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends InitializationStore
    with DbStore {
  import storage.api.*

  override def uid(implicit traceContext: TraceContext): Future[Option[UniqueIdentifier]] =
    storage.query(
      for {
        data <- idQuery
      } yield data.headOption.map { case (identity, fingerprint) =>
        UniqueIdentifier.tryCreate(identity, Namespace(fingerprint))
      },
      functionFullName,
    )

  private val idQuery =
    sql"select identifier, namespace from common_node_id"
      .as[(String, Fingerprint)]

  override def setUid(id: UniqueIdentifier)(implicit traceContext: TraceContext): Future[Unit] =
    storage.queryAndUpdate(
      {
        for {
          storedData <- idQuery
          _ <-
            if (storedData.nonEmpty) {
              val data = storedData(0)
              val prevNodeId = UniqueIdentifier.tryCreate(data._1, Namespace(data._2))
              ErrorUtil.requireArgument(
                prevNodeId == id,
                s"Unique id of node is already defined as $prevNodeId and can't be changed to $id!",
              )
              DbStorage.DbAction.unit
            } else
              sqlu"insert into common_node_id(identifier, namespace) values(${id.identifier},${id.fingerprint})"
        } yield ()
      }.transactionally.withTransactionIsolation(Serializable),
      functionFullName,
    )

  override def throwIfNotDev(implicit traceContext: TraceContext): Future[Boolean] =
    storage
      .query(sql"SELECT test_column FROM common_node_id".as[Int], functionFullName)
      .map(_ => true)

}
