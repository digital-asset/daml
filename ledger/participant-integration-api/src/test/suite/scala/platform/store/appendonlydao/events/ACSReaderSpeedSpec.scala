// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class ACSReaderSpeedSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {
  private val actorSystem = ActorSystem()
  private implicit val materializer: Materializer = Materializer(actorSystem)
  private implicit val ec: ExecutionContext = actorSystem.dispatcher
  private implicit val lc: LoggingContext = LoggingContext.ForTesting

  override def afterAll(): Unit = {
    Await.result(actorSystem.terminate(), Duration(10, "seconds"))
    ()
  }

  behavior of "speed"

  it should "speed" in {
    val filterSize = 10
    val range = 1000000
    val metrics = Metrics.fromSharedMetricRegistries("lalala")
    def reader(f: Map[String, AtomicReference[List[Long]]]) = new TestACSReader(
      idPageSize = 20000,
      pageSize = 1000,
      idFetchingParallelism = 100,
      metrics = metrics,
      materializer = materializer,
      fixture = f,
    )

    println("Creating fixtures...")
    val filters: FilterRelation =
      1.to(filterSize)
        .map(i => Ref.Party.assertFromString(s"party$i") -> Set.empty[Ref.Identifier])
        .toMap
    val redundancy = 10
    val baseFixtures =
      0.until(filterSize / redundancy)
        .map(i => (i + 1).to(range, filterSize / redundancy).map(_.toLong).toList)
        .toVector
    def fixture =
      1.to(filterSize)
        .map(i => s"party$i" -> new AtomicReference(baseFixtures(i % baseFixtures.size)))
        .toMap

    def runTest(test: TestACSReader => Source[Vector[Long], NotUsed]): Future[(Long, Long)] = {
      val start = System.nanoTime()
      test(reader(fixture))
        .runFold(0L) { case (sum, batch) =>
//            println(batch.size)
          sum + batch.size
        }
        .map(_ -> (System.nanoTime() - start))
    }
    def runTest1 = runTest(_.acsStream1(filters))
    def runTest2 = runTest(_.acsStream2(filters))
    def runTest3 = runTest(_.acsStream3(filters))
    val allIds = fixture.valuesIterator.map(_.get().size.toLong).sum
    println(s"all ids: $allIds")
    def repeat(baseLineResult: Long, times: Int)(test: => Future[(Long, Long)]): Future[Unit] = {
      def go(avg: Long, left: Int): Future[Long] =
        if (left == 0) Future.successful(avg)
        else
          test.flatMap { case (result, took) =>
            println(s"         ${took / 1000000}ms ")
            result shouldBe baseLineResult
            go(avg + (took / times), left - 1)
          }
      go(0, times).map(avg =>
        println(
          s"\n  AVG: ${avg / 1000000}ms    ${allIds * 1000000000 / avg} id/s     ${baseLineResult * 1000000000 / avg} merged_unique_id/s"
        )
      )
    }

    println("Getting the first result...")
    for {
      (baseLineResult, _) <- runTest1
      _ = println(s"$baseLineResult")
      _ = println("Warmup3")
      _ <- repeat(baseLineResult, 2)(runTest3)
      _ = println("Test3")
      _ <- repeat(baseLineResult, 5)(runTest3)
      _ = println("Warmup1")
      _ <- repeat(baseLineResult, 2)(runTest1)
      _ = println("Test1")
      _ <- repeat(baseLineResult, 5)(runTest1)
      _ = println("Warmup2")
      _ <- repeat(baseLineResult, 2)(runTest2)
      _ = println("Test2")
      _ <- repeat(baseLineResult, 5)(runTest2)
    } yield succeed
  }
}
