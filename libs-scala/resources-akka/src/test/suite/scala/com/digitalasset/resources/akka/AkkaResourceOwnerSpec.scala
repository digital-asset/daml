// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.daml.resources.ResourceOwner
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.{Future, Promise}

class AkkaResourceOwnerSpec extends AsyncWordSpec with Matchers {
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

      val resource = for {
        actorSystem <- AkkaResourceOwner
          .forActorSystem(() => ActorSystem("TestActorSystem"))
          .acquire()
        actor <- ResourceOwner
          .successful(actorSystem.actorOf(Props(new TestActor)))
          .acquire()
      } yield (actorSystem, actor)

      for {
        resourceFuture <- resource.asFuture
        (actorSystem, actor) = resourceFuture
        _ = actor ! 7
        result <- testPromise.future
        _ <- resource.release()
      } yield {
        result should be(7)
        an[IllegalStateException] should be thrownBy actorSystem.actorOf(Props(new TestActor))
      }
    }
  }

  "a function returning a Materializer" should {
    "convert to a ResourceOwner" in {
      val resource = for {
        actorSystem <- AkkaResourceOwner
          .forActorSystem(() => ActorSystem("TestActorSystem"))
          .acquire()
        materializer <- AkkaResourceOwner.forMaterializer(() => Materializer(actorSystem)).acquire()
      } yield materializer

      for {
        materializer <- resource.asFuture
        numbers <- Source(1 to 10)
          .toMat(Sink.seq)(Keep.right[NotUsed, Future[Seq[Int]]])
          .run()(materializer)
        _ <- resource.release()
      } yield {
        numbers should be(1 to 10)
        an[IllegalStateException] should be thrownBy Source
          .single(0)
          .toMat(Sink.ignore)(Keep.right[NotUsed, Future[Done]])
          .run()(materializer)
      }
    }
  }
}
