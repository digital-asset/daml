// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class ACSReaderSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {
  private val actorSystem = ActorSystem()
  private implicit val materializer: Materializer = Materializer(actorSystem)
  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  override def afterAll(): Unit = {
    Await.result(actorSystem.terminate(), Duration(10, "seconds"))
    ()
  }

  behavior of "idSource"

  it should "populate expected continuation" in {
    FilterTableACSReader
      .idSource(pageBufferSize = 1) { from =>
        Future.successful(
          if (from == 8) Vector.empty
          else Vector(from + 1, from + 1, from + 2)
        )
      }
      .runWith(Sink.seq)
      .map(_ shouldBe Vector(1, 1, 2, 3, 3, 4, 5, 5, 6, 7, 7, 8))
  }

  it should "populate empty stream correctly" in {
    FilterTableACSReader
      .idSource(pageBufferSize = 1) { _ =>
        Future.successful(
          Vector.empty
        )
      }
      .runWith(Sink.seq)
      .map(_ shouldBe Vector.empty)
  }

  behavior of "mergeSort"

  it should "sort correctly zero sources" in testMergeSort {
    Vector.empty
  }

  it should "sort correctly one source" in testMergeSort {
    Vector(
      sortedRandomInts(10)
    )
  }

  it should "sort correctly one empty source" in testMergeSort {
    Vector(
      sortedRandomInts(0)
    )
  }

  it should "sort correctly 2 sources with same size" in testMergeSort {
    Vector(
      sortedRandomInts(10),
      sortedRandomInts(10),
    )
  }

  it should "sort correctly 2 sources with different size" in testMergeSort {
    Vector(
      sortedRandomInts(5),
      sortedRandomInts(10),
    )
  }

  it should "sort correctly 2 sources one of them empty" in testMergeSort {
    Vector(
      sortedRandomInts(0),
      sortedRandomInts(10),
    )
  }

  it should "sort correctly 2 sources both of them empty" in testMergeSort {
    Vector(
      sortedRandomInts(0),
      sortedRandomInts(0),
    )
  }

  it should "sort correctly 10 sources, random size" in testMergeSort(
    {
      Vector.fill(10)(sortedRandomInts(10))
    },
    times = 100,
  )

  behavior of "statefulDeduplicate"

  it should "deduplicate a stream correctly" in {
    Source(Vector(1, 1, 2, 2, 2, 3, 4, 4, 5, 6, 7, 0, 0, 0))
      .statefulMapConcat(FilterTableACSReader.statefulDeduplicate)
      .runWith(Sink.seq)
      .map(_ shouldBe Vector(1, 2, 3, 4, 5, 6, 7, 0))
  }

  it should "preserve a stream of unique numbers" in {
    Source(Vector(1, 2, 3, 4, 5, 6, 7, 0))
      .statefulMapConcat(FilterTableACSReader.statefulDeduplicate)
      .runWith(Sink.seq)
      .map(_ shouldBe Vector(1, 2, 3, 4, 5, 6, 7, 0))
  }

  it should "work for empty stream" in {
    Source(Vector.empty)
      .statefulMapConcat(FilterTableACSReader.statefulDeduplicate)
      .runWith(Sink.seq)
      .map(_ shouldBe Vector.empty)
  }

  it should "work for one sized stream" in {
    Source(Vector(1))
      .statefulMapConcat(FilterTableACSReader.statefulDeduplicate)
      .runWith(Sink.seq)
      .map(_ shouldBe Vector(1))
  }

  it should "work if only duplications present" in {
    Source(Vector(1, 1, 1, 1))
      .statefulMapConcat(FilterTableACSReader.statefulDeduplicate)
      .runWith(Sink.seq)
      .map(_ shouldBe Vector(1))
  }

  private def sortedRandomInts(length: Int): Vector[Int] =
    Vector.fill(length)(scala.util.Random.nextInt(10)).sorted

  private def testMergeSort(in: => Vector[Vector[Int]], times: Int = 5): Future[Assertion] = {
    val testInput = in
    FilterTableACSReader
      .mergeSort[Int](
        sources = testInput.map(Source.apply)
      )
      .runWith(Sink.seq)
      .map(_ shouldBe testInput.flatten.sorted)
      .flatMap { result =>
        if (times == 0) Future.successful(result)
        else testMergeSort(in, times - 1)
      }
  }

}
