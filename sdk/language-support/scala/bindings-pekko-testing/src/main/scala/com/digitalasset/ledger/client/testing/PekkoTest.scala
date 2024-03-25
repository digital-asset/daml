// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.testing

import java.util
import java.util.concurrent.{Executors, ScheduledExecutorService}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.{ActorSystem, Scheduler}
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString
import com.daml.grpc.adapter.{ExecutionSequencerFactory, SingleThreadExecutionSequencerPool}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.control.NonFatal

trait PekkoTest extends BeforeAndAfterAll with LazyLogging { self: Suite =>
  // TestEventListener is needed for log testing
  private val loggers =
    util.Arrays.asList(
      "org.apache.pekko.event.slf4j.Slf4jLogger",
      "org.apache.pekko.testkit.TestEventListener",
    )
  protected implicit val sysConfig: Config = ConfigFactory
    .load()
    .withValue("pekko.loggers", ConfigValueFactory.fromIterable(loggers))
    .withValue("pekko.logger-startup-timeout", ConfigValueFactory.fromAnyRef("30s"))
    .withValue("pekko.stdout-loglevel", ConfigValueFactory.fromAnyRef("INFO"))
  protected implicit val system: ActorSystem = ActorSystem("test", sysConfig)
  protected implicit val ec: ExecutionContextExecutor =
    system.dispatchers.lookup("test-dispatcher")
  protected implicit val scheduler: Scheduler = system.scheduler
  protected implicit val schedulerService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor()
  protected implicit val materializer: Materializer = Materializer(system)
  protected implicit val esf: ExecutionSequencerFactory =
    new SingleThreadExecutionSequencerPool("testSequencerPool")
  protected val timeout: FiniteDuration = 2.minutes
  protected val shortTimeout: FiniteDuration = 5.seconds

  protected def await[T](fun: => Future[T]): T = Await.result(fun, timeout)

  protected def awaitShort[T](fun: => Future[T]): T = Await.result(fun, shortTimeout)

  protected def drain(source: Source[ByteString, NotUsed]): ByteString = {
    val futureResult: Future[ByteString] = source.runFold(ByteString.empty) { (a, b) =>
      a.concat(b)
    }
    awaitShort(futureResult)
  }

  protected def drain[A, B](source: Source[A, B]): Seq[A] = {
    val futureResult: Future[Seq[A]] = source.runWith(Sink.seq)
    awaitShort(futureResult)
  }

  override protected def afterAll(): Unit = {
    try {
      val _ = await(system.terminate())
    } catch {
      case NonFatal(_) => ()
    }
    schedulerService.shutdownNow()
    super.afterAll()
  }
}
