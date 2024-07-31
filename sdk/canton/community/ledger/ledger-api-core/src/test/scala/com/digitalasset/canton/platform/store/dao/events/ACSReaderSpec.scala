// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.digitalasset.canton.platform.store.dao.events.EventIdsUtils.*
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

class ACSReaderSpec extends AsyncFlatSpec with BaseTest with BeforeAndAfterAll {

  private val actorSystem = ActorSystem()
  private implicit val materializer: Materializer = Materializer(actorSystem)
  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  override def afterAll(): Unit = {
    Await.result(actorSystem.terminate(), 10.seconds)
    ()
  }

  behavior of "IdQueryConfiguration"

  it should "compute correct parameters for a realistic case" in {
    def realisticConfigForFilterSize(filterSize: Int) = IdPageSizing.calculateFrom(
      maxIdPageSize = 10000,
      workingMemoryInBytesForIdPages = 100 * 1024 * 1024,
      numOfDecomposedFilters = filterSize,
      numOfPagesInIdPageBuffer = 1,
      loggerFactory = loggerFactory,
    )
    // progression: 200 800 3200 10000 10000...
    realisticConfigForFilterSize(1) shouldBe IdPageSizing(200, 10000)
    realisticConfigForFilterSize(10) shouldBe IdPageSizing(200, 10000)
    realisticConfigForFilterSize(100) shouldBe IdPageSizing(200, 10000)
    // 200 800 3200 6553 6553...
    realisticConfigForFilterSize(1000) shouldBe IdPageSizing(200, 6553)
    // 200 655 655...
    realisticConfigForFilterSize(10000) shouldBe IdPageSizing(200, 655)
    loggerFactory.assertLogs(
      within = {
        realisticConfigForFilterSize(100000) shouldBe IdPageSizing(65, 65)
        realisticConfigForFilterSize(1000000) shouldBe IdPageSizing(10, 10)
        realisticConfigForFilterSize(10000000) shouldBe IdPageSizing(10, 10)
      },
      assertions = _.warningMessage should include(
        "Calculated maximum ID page size supporting API stream memory limits [65] is very low"
      ),
      _.warningMessage should include(
        "Calculated maximum ID page size supporting API stream memory limits [6] is too low"
      ),
      _.warningMessage should include(
        "Calculated maximum ID page size supporting API stream memory limits [0] is too low"
      ),
    )

  }

  it should "compute correct parameters, if maxIdPageSize is lower than recommended (200), then maxIdPageSize is preferred" in {
    def configWith(filterSize: Int) = IdPageSizing.calculateFrom(
      maxIdPageSize = 150,
      workingMemoryInBytesForIdPages = 100 * 1024 * 1024,
      numOfDecomposedFilters = filterSize,
      numOfPagesInIdPageBuffer = 1,
      loggerFactory = loggerFactory,
    )
    configWith(1) shouldBe IdPageSizing(150, 150)
    configWith(10) shouldBe IdPageSizing(150, 150)
    configWith(100) shouldBe IdPageSizing(150, 150)
    configWith(1000) shouldBe IdPageSizing(150, 150)
    configWith(10000) shouldBe IdPageSizing(150, 150)
    loggerFactory.assertLogs(
      within = {
        configWith(100000) shouldBe IdPageSizing(65, 65)
        configWith(1000000) shouldBe IdPageSizing(10, 10)
        configWith(10000000) shouldBe IdPageSizing(10, 10)
      },
      assertions = _.warningMessage should include(
        "Calculated maximum ID page size supporting API stream memory limits [65] is very low"
      ),
      _.warningMessage should include(
        "Calculated maximum ID page size supporting API stream memory limits [6] is too low"
      ),
      _.warningMessage should include(
        "Calculated maximum ID page size supporting API stream memory limits [0] is too low"
      ),
    )
  }

  it should "compute correct parameters, if maxIdPageSize is lower than minimum (10), then maxIdPageSize is preferred" in {
    def configWith(filterSize: Int) = IdPageSizing.calculateFrom(
      maxIdPageSize = 4,
      workingMemoryInBytesForIdPages = 100 * 1024 * 1024,
      numOfDecomposedFilters = filterSize,
      numOfPagesInIdPageBuffer = 1,
      loggerFactory = loggerFactory,
    )
    configWith(1) shouldBe IdPageSizing(4, 4)
    configWith(10) shouldBe IdPageSizing(4, 4)
    configWith(100) shouldBe IdPageSizing(4, 4)
    configWith(1000) shouldBe IdPageSizing(4, 4)
    configWith(10000) shouldBe IdPageSizing(4, 4)
    configWith(100000) shouldBe IdPageSizing(4, 4)
    configWith(1000000) shouldBe IdPageSizing(4, 4)
    loggerFactory.assertLogs(
      configWith(10000000) shouldBe IdPageSizing(4, 4),
      _.warningMessage should include(
        "Calculated maximum ID page size supporting API stream memory limits [0] is too low"
      ),
    )
  }

  behavior of "idSource"

  it should "stream data exponentially" in {
    testIdSource(
      IdPageSizing(
        minPageSize = 1,
        maxPageSize = 20,
      ),
      Range(1, 70).map(_.toLong).toVector,
    ).map(
      _ shouldBe Vector(
        IdPaginationState(0, 1),
        IdPaginationState(1, 4),
        IdPaginationState(5, 16),
        IdPaginationState(21, 20),
        IdPaginationState(41, 20),
        IdPaginationState(61, 20),
        IdPaginationState(69, 20),
      )
    )
  }

  it should "stream data constantly" in {
    testIdSource(
      IdPageSizing(
        minPageSize = 20,
        maxPageSize = 20,
      ),
      Range(1, 70).map(_.toLong).toVector,
    ).map(
      _ shouldBe Vector(
        IdPaginationState(0, 20),
        IdPaginationState(20, 20),
        IdPaginationState(40, 20),
        IdPaginationState(60, 20),
        IdPaginationState(69, 20),
      )
    )
  }

  it should "stream data exponentially, if maxPageSize never reached" in {
    testIdSource(
      IdPageSizing(
        minPageSize = 1,
        maxPageSize = 20,
      ),
      Range(1, 6).map(_.toLong).toVector,
    ).map(
      _ shouldBe Vector(
        IdPaginationState(0, 1),
        IdPaginationState(1, 4),
        IdPaginationState(5, 16),
      )
    )
  }

  it should "stream empty data" in {
    testIdSource(
      IdPageSizing(
        minPageSize = 1,
        maxPageSize = 20,
      ),
      Vector.empty,
    ).map(
      _ shouldBe Vector(
        IdPaginationState(0, 1)
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
    Vector.fill(10)(sortedRandomInts(10)),
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
    EventIdsUtils
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
      idQueryConfiguration: IdPageSizing,
      ids: Vector[Long],
  ): Future[Vector[IdPaginationState]] = {
    val queries = Vector.newBuilder[IdPaginationState]
    paginatingAsyncStream
      .streamIdsFromSeekPagination(idQueryConfiguration, 1, 0L) { idQuery =>
        queries.addOne(idQuery)
        Future.successful(
          ids
            .dropWhile(_ <= idQuery.fromIdExclusive)
            .take(idQuery.pageSize)
        )
      }
      .runWith(Sink.seq[Long])
      .map { result =>
        result shouldBe ids
        queries.result()
      }
  }

}
