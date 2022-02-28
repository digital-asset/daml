// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index.internal

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{Materializer, RestartSettings}
import akka.{Done, NotUsed}
import com.daml.logging.LoggingContext
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

class RestartableManagedStreamSpec
    extends AsyncFlatSpec
    with Matchers
    with Eventually
    with BeforeAndAfterAll {
  private val actorSystem = ActorSystem("test")
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private implicit val materializer: Materializer = Materializer(actorSystem)

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2000, Millis)), interval = scaled(Span(50, Millis)))

  private val restartSettings = RestartSettings(
    minBackoff = 1.millis,
    maxBackoff = 1.seconds,
    randomFactor = 0.0,
  )

  behavior of classOf[RestartableManagedStream[_]].getSimpleName

  it should "feed elements from the source to the sink" in {
    val input = 1 to 5
    val outputCapture = ArrayBuffer.empty[Int]
    val restartableManagedSubscription = managedSubscription[Int](
      streamBuilder = () => Source.fromIterator(() => input.iterator).concat(Source.never),
      sinkConsume = Sink.foreach(outputCapture += _),
    )

    for {
      _ <- eventually(
        Future {
          outputCapture should contain theSameElementsInOrderAs input
        }
      )
      _ <- restartableManagedSubscription.release()
    } yield succeed
  }

  it should "restart if the source fails" in {
    val input = 1 to 5
    val inputIterator = input.iterator
    val outputCapture = ArrayBuffer.empty[Int]
    val restartableManagedSubscription = managedSubscription[Option[Int]](
      streamBuilder = () =>
        Source
          .tick(1.milli, 1.millis, ())
          .map(_ => inputIterator.nextOption())
          .map {
            case Some(3) =>
              // Fail on some stream element
              throw new RuntimeException("Stream failed")
            case other => other
          }
          .mapMaterializedValue(_ => NotUsed)
          .concat(Source.never),
      sinkConsume = Sink.foreach(_.foreach(outputCapture += _)),
    )

    for {
      _ <- eventually(
        Future {
          // It skipped the 3rd tick but continued after restart
          outputCapture should contain theSameElementsInOrderAs input.filterNot(_ == 3)
        }
      )
      _ <- restartableManagedSubscription.release()
    } yield succeed
  }

  it should "fail if the consumer fails" in {
    val failedMessage = "Consumer failed"
    val sysOutput = new AtomicInteger(0)
    val input = 1 to 5
    val outputCapture = ArrayBuffer.empty[Int]
    val restartableManagedSubscription = managedSubscription(
      streamBuilder = () => Source.fromIterator(() => input.iterator).concat(Source.never),
      sinkConsume = Sink.foreach[Int] {
        case 3 =>
          // Fail on some stream element
          throw new RuntimeException(failedMessage)
        case tick => outputCapture += tick
      },
      teardown = sysOutput.set,
    )

    for {
      _ <-
        eventually(
          Future {
            // It failed after the second tick
            outputCapture should contain theSameElementsInOrderAs Seq(1, 2)
            sysOutput.get shouldBe 1
          }
        )
      _ <- restartableManagedSubscription.release().recoverWith {
        case NonFatal(e) if e.getMessage.contains(failedMessage) => Future(succeed)
        case other => Future.failed[Assertion](other)
      }
    } yield succeed
  }

  private def managedSubscription[Out](
      streamBuilder: () => Source[Out, NotUsed],
      sinkConsume: Sink[Out, Future[Done]],
      teardown: Int => Unit = _ => fail("should not be triggered"),
  ): RestartableManagedStream[Out] =
    new RestartableManagedStream[Out](
      "test stream",
      sourceBuilder = streamBuilder,
      sink = sinkConsume,
      restartSettings = restartSettings,
      teardown = teardown,
    )
}
