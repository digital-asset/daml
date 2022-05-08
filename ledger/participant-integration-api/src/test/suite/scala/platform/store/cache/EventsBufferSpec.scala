// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

//import akka.actor.ActorSystem
//import akka.stream.Materializer
//import akka.stream.scaladsl.Source
//import com.codahale.metrics.MetricRegistry
//import com.daml.ledger.offset.Offset
//import com.daml.metrics.Metrics
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.should.Matchers
//import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
//
//import scala.concurrent.duration._
//import scala.concurrent.{Await, ExecutionContext, Future}
//import scala.util.Random
//
//class EventsBufferSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {
//
//  it should "not hold memory" in {
//    val actorSystem = ActorSystem()
//    implicit val materializer: Materializer = Materializer(actorSystem)
//    val maxBufferSize = 1000
//    val maxFetchSize = 100
//    val eventsBuffer = new EventsBuffer[String](
//      maxBufferSize = maxBufferSize.toLong,
//      maxFetchSize = maxFetchSize,
//      bufferQualifier = "test",
//      ignoreMarker = _ => false,
//      metrics = new Metrics(new MetricRegistry()),
//    )
//
//    implicit val ec: ExecutionContext = ExecutionContext.global
//
//    val r = Future.sequence((1 to 10).map { epoch =>
//      (1 to maxBufferSize).foreach { tiny =>
//        val idx = epoch * maxBufferSize + tiny
//        val str = Random.nextString(102400)
//        eventsBuffer.push(offset(idx), str)
//      }
//
//      eventsBuffer.slice(
//        offset(epoch * maxBufferSize),
//        offset((epoch + 1) * maxBufferSize),
//        e => Future.successful(Some(e)),
//        _ => () => Source.empty,
//      ) match {
//        case BufferSlice.Inclusive(source) =>
//          source
//            .map(e => {
//              Thread.sleep(100L)
//              e
//            })
//            .map(_._2)
//            .map(_ => 1)
//            .reduce(_ + _)
//            .runForeach(println)
//        case BufferSlice.Prefix(_, source) =>
//          source
//            .map(e => {
//              Thread.sleep(100L)
//              e
//            })
//            .map(_._2)
//            .map(_ => 1)
//            .reduce(_ + _)
//            .runForeach(println)
//        case _ => Future.unit
//      }
//    })
//
//    println("Finished ingestion")
//
//    Await.ready(r, 1000.seconds)
//
//    materializer.shutdown()
//    actorSystem.terminate()
//  }
//
//  private def offset(idx: Int): Offset = {
//    val base = BigInt(1L) << 32
//    Offset.fromByteArray((base + idx.toLong).toByteArray)
//  }
//}
