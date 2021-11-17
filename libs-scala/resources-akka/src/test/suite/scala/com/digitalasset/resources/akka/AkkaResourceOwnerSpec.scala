// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.daml.resources.{HasExecutionContext, ResourceOwnerFactories, TestContext}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{Future, Promise}

final class AkkaResourceOwnerSpec extends AsyncWordSpec with Matchers {
  private val Factories = new ResourceOwnerFactories[TestContext]
    with AkkaResourceOwnerFactories[TestContext] {
    override protected implicit val hasExecutionContext: HasExecutionContext[TestContext] =
      TestContext.`TestContext has ExecutionContext`
  }
  private implicit val context: TestContext = new TestContext(executionContext)

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

      resourceOwner
        .use { case (actorSystem, actor) =>
          actor ! 7
          testPromise.future.map { result =>
            result should be(7)
            actorSystem
          }
        }
        .map { actorSystem =>
          an[IllegalStateException] should be thrownBy actorSystem.actorOf(
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

      resourceOwner
        .use { materializer =>
          Source(1 to 10)
            .toMat(Sink.seq)(Keep.right[NotUsed, Future[Seq[Int]]])
            .run()(materializer)
            .map { numbers =>
              numbers should be(1 to 10)
              materializer
            }
        }
        .map { materializer =>
          an[IllegalStateException] should be thrownBy Source
            .single(0)
            .toMat(Sink.ignore)(Keep.right[NotUsed, Future[Done]])
            .run()(materializer)
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

      resourceOwner
        .use(_ => cancellable.isCancelled should be(false))
        .map(_ => cancellable.isCancelled should be(true))
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

      resourceOwner
        .use { sourceQueue =>
          sourceQueue.offer(123)
        }
        .map { offerResult =>
          offerResult should be(QueueOfferResult.Enqueued)
          number.get should be(123)
        }
    }
  }
}
