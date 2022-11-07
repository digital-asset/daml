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
import ACSReader._
import com.daml.logging.LoggingContext

class ACSReaderSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {
  private val actorSystem = ActorSystem()
  private implicit val materializer: Materializer = Materializer(actorSystem)
  private implicit val ec: ExecutionContext = actorSystem.dispatcher
  private implicit val lc: LoggingContext = LoggingContext.empty

  override def afterAll(): Unit = {
    Await.result(actorSystem.terminate(), Duration(10, "seconds"))
    ()
  }

  behavior of "IdQueryConfiguration"

  it should "compute correct parameters for a realistic case" in {
    def realisticConfigForFilterSize(filterSize: Int) = IdQueryPageSizing.calculateFrom(
      maxNumberOfIdsPerIdPage = 10000,
      maxTotalWorkingMemoryInBytesForIdPages = 100 * 1024 * 1024,
      maxNumberOfDecomposedFilters = filterSize,
      maxNumberOfPagesPerIdPageBuffer = 1,
    )
    // progression: 200 800 3200 10000 10000...
    realisticConfigForFilterSize(1) shouldBe IdQueryPageSizing(200, 10000)
    realisticConfigForFilterSize(10) shouldBe IdQueryPageSizing(200, 10000)
    realisticConfigForFilterSize(100) shouldBe IdQueryPageSizing(200, 10000)
    // 200 800 3200 6553 6553...
    realisticConfigForFilterSize(1000) shouldBe IdQueryPageSizing(200, 6553)
    // 200 655 655...
    realisticConfigForFilterSize(10000) shouldBe IdQueryPageSizing(200, 655)
    realisticConfigForFilterSize(100000) shouldBe IdQueryPageSizing(65, 65)
    realisticConfigForFilterSize(1000000) shouldBe IdQueryPageSizing(10, 10)
    realisticConfigForFilterSize(10000000) shouldBe IdQueryPageSizing(10, 10)
  }

  it should "compute correct parameters, if maxIdPageSize is lower than recommended (200), then maxIdPageSize is preferred" in {
    def configWith(filterSize: Int) = IdQueryPageSizing.calculateFrom(
      maxNumberOfIdsPerIdPage = 150,
      maxTotalWorkingMemoryInBytesForIdPages = 100 * 1024 * 1024,
      maxNumberOfDecomposedFilters = filterSize,
      maxNumberOfPagesPerIdPageBuffer = 1,
    )
    configWith(1) shouldBe IdQueryPageSizing(150, 150)
    configWith(10) shouldBe IdQueryPageSizing(150, 150)
    configWith(100) shouldBe IdQueryPageSizing(150, 150)
    configWith(1000) shouldBe IdQueryPageSizing(150, 150)
    configWith(10000) shouldBe IdQueryPageSizing(150, 150)
    configWith(100000) shouldBe IdQueryPageSizing(65, 65)
    configWith(1000000) shouldBe IdQueryPageSizing(10, 10)
    configWith(10000000) shouldBe IdQueryPageSizing(10, 10)
  }

  it should "compute correct parameters, if maxIdPageSize is lower than minimum (10), then maxIdPageSize is preferred" in {
    def configWith(filterSize: Int) = IdQueryPageSizing.calculateFrom(
      maxNumberOfIdsPerIdPage = 4,
      maxTotalWorkingMemoryInBytesForIdPages = 100 * 1024 * 1024,
      maxNumberOfDecomposedFilters = filterSize,
      maxNumberOfPagesPerIdPageBuffer = 1,
    )
    configWith(1) shouldBe IdQueryPageSizing(4, 4)
    configWith(10) shouldBe IdQueryPageSizing(4, 4)
    configWith(100) shouldBe IdQueryPageSizing(4, 4)
    configWith(1000) shouldBe IdQueryPageSizing(4, 4)
    configWith(10000) shouldBe IdQueryPageSizing(4, 4)
    configWith(100000) shouldBe IdQueryPageSizing(4, 4)
    configWith(1000000) shouldBe IdQueryPageSizing(4, 4)
    configWith(10000000) shouldBe IdQueryPageSizing(4, 4)
  }

  behavior of "idSource"

  it should "stream data exponentially" in {
    testIdSource(
      IdQueryPageSizing(
        minNumberOfIdsPerPage = 1,
        maxNumberOfIdsPerPage = 20,
      ),
      Range(1, 70).map(_.toLong).toVector,
    ).map(
      _ shouldBe Vector(
        IdQueryParams(0, 1),
        IdQueryParams(1, 4),
        IdQueryParams(5, 16),
        IdQueryParams(21, 20),
        IdQueryParams(41, 20),
        IdQueryParams(61, 20),
        IdQueryParams(69, 20),
      )
    )
  }

  it should "stream data constantly" in {
    testIdSource(
      IdQueryPageSizing(
        minNumberOfIdsPerPage = 20,
        maxNumberOfIdsPerPage = 20,
      ),
      Range(1, 70).map(_.toLong).toVector,
    ).map(
      _ shouldBe Vector(
        IdQueryParams(0, 20),
        IdQueryParams(20, 20),
        IdQueryParams(40, 20),
        IdQueryParams(60, 20),
        IdQueryParams(69, 20),
      )
    )
  }

  it should "stream data exponentially, if maxPageSize never reached" in {
    testIdSource(
      IdQueryPageSizing(
        minNumberOfIdsPerPage = 1,
        maxNumberOfIdsPerPage = 20,
      ),
      Range(1, 6).map(_.toLong).toVector,
    ).map(
      _ shouldBe Vector(
        IdQueryParams(0, 1),
        IdQueryParams(1, 4),
        IdQueryParams(5, 16),
      )
    )
  }

  it should "stream empty data" in {
    testIdSource(
      IdQueryPageSizing(
        minNumberOfIdsPerPage = 1,
        maxNumberOfIdsPerPage = 20,
      ),
      Vector.empty,
    ).map(
      _ shouldBe Vector(
        IdQueryParams(0, 1)
      )
    )
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
      .statefulMapConcat(statefulDeduplicate)
      .runWith(Sink.seq)
      .map(_ shouldBe Vector(1, 2, 3, 4, 5, 6, 7, 0))
  }

  it should "preserve a stream of unique numbers" in {
    Source(Vector(1, 2, 3, 4, 5, 6, 7, 0))
      .statefulMapConcat(statefulDeduplicate)
      .runWith(Sink.seq)
      .map(_ shouldBe Vector(1, 2, 3, 4, 5, 6, 7, 0))
  }

  it should "work for empty stream" in {
    Source(Vector.empty)
      .statefulMapConcat(statefulDeduplicate)
      .runWith(Sink.seq)
      .map(_ shouldBe Vector.empty)
  }

  it should "work for one sized stream" in {
    Source(Vector(1))
      .statefulMapConcat(statefulDeduplicate)
      .runWith(Sink.seq)
      .map(_ shouldBe Vector(1))
  }

  it should "work if only duplications present" in {
    Source(Vector(1, 1, 1, 1))
      .statefulMapConcat(statefulDeduplicate)
      .runWith(Sink.seq)
      .map(_ shouldBe Vector(1))
  }

  private def sortedRandomInts(length: Int): Vector[Int] =
    Vector.fill(length)(scala.util.Random.nextInt(10)).sorted

  private def testMergeSort(in: => Vector[Vector[Int]], times: Int = 5): Future[Assertion] = {
    val testInput = in
    ACSReader
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

  private def testIdSource(
      idQueryConfiguration: IdQueryPageSizing,
      ids: Vector[Long],
  ): Future[Vector[IdQueryParams]] = {
    val queries = Vector.newBuilder[IdQueryParams]
    streamIdsFromSeekPagination(idQueryConfiguration, 1) { idQuery =>
      queries.addOne(idQuery)
      Future.successful(
        ids
          .dropWhile(_ <= idQuery.fromExclusiveEventSeqId)
          .take(idQuery.numberOfIdsToFetch)
          .toArray
      )
    }.runWith(Sink.seq[Long]).map { result =>
      result shouldBe ids
      queries.result()
    }
  }

}
