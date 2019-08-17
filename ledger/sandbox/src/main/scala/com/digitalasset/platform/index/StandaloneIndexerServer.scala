// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.actor.ActorSystem
import akka.pattern.after
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
object StandaloneIndexerServer {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private[this] val restartDelay: FiniteDuration = 5.seconds
  private[this] val actorSystem = ActorSystem("StandaloneIndexerServer")

  def apply(readService: ReadService, jdbcUrl: String): AutoCloseable = {

    val closed = new AtomicBoolean(false)
    val lastHandle = new AtomicReference[Option[IndexFeedHandle]](None)

    def restart(): Future[akka.Done] = {
      logger.info("Starting Indexer Server")
      implicit val ec: ExecutionContext = DEC
      val completedF = for {
        server <- JdbcIndexer.create(readService, jdbcUrl)
        handle <- server.subscribe(readService)
        _ = {
          logger.info("Started Indexer Server")
          lastHandle.set(Some(handle))
        }
        completed <- handle.completed()
        _ = logger.info("Successfully finished processing state updates")
      } yield completed

      completedF.recoverWith {
        case NonFatal(t) =>
          logger.error(s"Error while running indexer, restart scheduled after $restartDelay", t)
          after(restartDelay, actorSystem.scheduler)(restart())
      }(DEC)
    }

    restart()

    new AutoCloseable {
      override def close(): Unit = {
        if (closed.compareAndSet(false, true)) {
          val _ =
            lastHandle.get.foreach(h => Await.result(h.stop(), JdbcIndexer.asyncTolerance))
        }
      }
    }
  }
}
