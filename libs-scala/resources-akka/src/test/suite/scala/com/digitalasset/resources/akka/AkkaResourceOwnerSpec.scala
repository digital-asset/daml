// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueue}
import akka.{Done, NotUsed}
import com.daml.resources.{HasExecutionContext, ResourceOwnerFactories, TestContext}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{Await, ExecutionContext, Future, Promise, blocking}
import scala.concurrent.duration.DurationInt

final class AkkaResourceOwnerSpec extends AnyWordSpec with Matchers {

  import AkkaResourceOwnerSpec._

  private implicit val context: TestContext = new TestContext(ExecutionContext.global)

  "a function returning an ActorSystem" should {
    "convert to a ResourceOwner" in {
      val testPromise = Promise[Int]()
      class TestActor extends Actor {
        @SuppressWarnings(Array("org.wartremover.warts.Any"))
        override def receive: Receive = {
          case value: Int => testPromise.success(value)
          case value => testPromise.failure(new IllegalArgumentException(s"$value"))
        }
      }

      val resourceOwner = for {
        actorSystem <- Factories.forActorSystem(() => ActorSystem("TestActorSystem"))
        actor <- Factories.successful(actorSystem.actorOf(Props(new TestActor)))
      } yield (actorSystem, actor)

      val actorSystemReference = new AtomicReference[ActorSystem]()

      val resultFuture = resourceOwner
        .use { case (actorSystem, actor) =>
          actorSystemReference.set(actorSystem)
          actor ! 7
          testPromise.future
        }

      blocking {
        val result = Await.result(resultFuture, TimeoutDuration)
        result should be(7)
        an[IllegalStateException] should be thrownBy actorSystemReference.get.actorOf(
          Props(new TestActor)
        )
      }
    }
  }

  "a function returning a Materializer" should {
    "convert to a ResourceOwner" in {
      val resourceOwner = for {
        actorSystem <- Factories.forActorSystem(() => ActorSystem("TestActorSystem"))
        materializer <- Factories.forMaterializer(() => Materializer(actorSystem))
      } yield materializer

      val materializerReference = new AtomicReference[Materializer]()

      val resultFuture = resourceOwner.use { materializer =>
        materializerReference.set(materializer)
        Source(1 to 10)
          .toMat(Sink.seq)(Keep.right[NotUsed, Future[Seq[Int]]])
          .run()(materializer)
      }

      blocking {
        val numbers = Await.result(resultFuture, TimeoutDuration)
        numbers should be(1 to 10)
        an[IllegalStateException] should be thrownBy Source
          .single(0)
          .toMat(Sink.ignore)(Keep.right[NotUsed, Future[Done]])
          .run()(materializerReference.get)
      }
    }
  }

  "a function returning a Cancellable" should {
    "convert to a ResourceOwner" in {
      val cancellable = new Cancellable {
        private val isCancelledAtomic = new AtomicBoolean

        override def cancel(): Boolean =
          isCancelledAtomic.compareAndSet(false, true)

        override def isCancelled: Boolean =
          isCancelledAtomic.get
      }
      val resourceOwner = Factories.forCancellable(() => cancellable)

      val resultFuture = resourceOwner.use { cancellable =>
        cancellable.isCancelled should be(false)
        Future.unit
      }

      blocking {
        Await.ready(resultFuture, TimeoutDuration)
        cancellable.isCancelled should be(true)
      }
    }
  }

  "a function returning a SourceQueue" should {
    "convert to a ResourceOwner" in {
      val number = new AtomicInteger()
      val resourceOwner = for {
        actorSystem <- Factories
          .forActorSystem(() => ActorSystem("TestActorSystem"))
        materializer <- Factories.forMaterializer(() => Materializer(actorSystem))
        sourceQueue <- Factories
          .forSourceQueue(
            Source
              .queue[Int](10, OverflowStrategy.backpressure)
              .to(Sink.foreach(number.set))
          )(materializer)
      } yield sourceQueue

      val sourceQueueReference = new AtomicReference[SourceQueue[Int]]()

      val resultFuture = resourceOwner.use { sourceQueue =>
        sourceQueueReference.set(sourceQueue)
        sourceQueue.offer(123)
      }

      blocking {
        val offerResult = Await.result(resultFuture, TimeoutDuration)
        offerResult should be(QueueOfferResult.Enqueued)
        number.get should be(123)
      }
    }
  }
}

object AkkaResourceOwnerSpec {
  private val TimeoutDuration = 2.seconds

  private val Factories = new ResourceOwnerFactories[TestContext]
    with AkkaResourceOwnerFactories[TestContext] {
    override protected implicit val hasExecutionContext: HasExecutionContext[TestContext] =
      TestContext.`TestContext has ExecutionContext`
  }
}
