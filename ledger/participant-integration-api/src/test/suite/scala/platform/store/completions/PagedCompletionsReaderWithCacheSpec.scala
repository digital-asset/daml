// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}

import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.Party
import com.daml.logging.LoggingContext
import com.daml.platform.store.completions.CompletionsDaoMock.{
  GetAllCompletionsRequest,
  GetFilteredCompletionsRequest,
  Request,
}
import com.daml.platform.store.dao.CommandCompletionsTable
import com.daml.platform.store.dao.CommandCompletionsTable.CompletionStreamResponseWithParties
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import com.daml.platform.store.completions.OffsetsGenerator.genOffset

class PagedCompletionsReaderWithCacheSpec extends AnyFlatSpec with Matchers {

  private implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  it should "correctly fetch historic data" in {

    // GIVEN: completions reader and dao mock with 20 completions
    val completionsDao = new CompletionsDaoMock(CompletionsDaoMock.genCompletions(20))
    val reader = new PagedCompletionsReaderWithCache(completionsDao, 10)

    // WHEN: completions are requested for the first time
    val cachedCompletions = await(
      reader.getCompletionsPage(
        genOffset(9),
        genOffset(19),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    // THEN returned completions size should be 10
    cachedCompletions.size shouldBe 10
    // AND all completions should be requested from database - proof of caching
    completionsDao.runQueriesJournalToSeq() shouldBe Seq(
      GetAllCompletionsRequest(genOffset(9), genOffset(19))
    )

    // WHEN: completions older than cached are fetched
    val historicCompletions = await(
      reader.getCompletionsPage(
        Offset.beforeBegin,
        genOffset(9),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    // THEN: 10 completions should be returned
    historicCompletions.size shouldBe 10

    // AND database should be queried with application and parties filter
    completionsDao.runQueriesJournalToSeq() shouldBe Seq[Request](
      GetAllCompletionsRequest(genOffset(9), genOffset(19)),
      GetFilteredCompletionsRequest(
        Offset.beforeBegin,
        genOffset(9),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      ),
    )
  }

  it should "correctly fetch future (not cached yet) data and replace oldest cache with it" in {

    // GIVEN: completions reader and dao mock with 20 completions
    val completionsDao = new CompletionsDaoMock(CompletionsDaoMock.genCompletions(30))

    val reader = new PagedCompletionsReaderWithCache(completionsDao, 10)

    // WHEN: not cached completions are fetched
    val cachedCompletions = await(
      reader.getCompletionsPage(
        genOffset(9),
        genOffset(19),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )
    // THEN: 10 completions should be returned
    cachedCompletions.size shouldBe 10

    // AND all completions should be requested from database - proof of caching
    completionsDao.runQueriesJournalToSeq() shouldBe Seq(
      GetAllCompletionsRequest(genOffset(9), genOffset(19))
    )

    // WHEN: newer completions are queried
    val futureCompletions = await(
      reader.getCompletionsPage(
        genOffset(9),
        genOffset(30),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )
    // THEN: 20 completions should be returned
    futureCompletions.size shouldBe 20

    // AND previously read completions should be read from cache
    // AND database should be queried only for new completions starting from offset 19
    completionsDao.runQueriesJournalToSeq() shouldBe Seq[Request](
      GetAllCompletionsRequest(genOffset(9), genOffset(19)),
      GetAllCompletionsRequest(genOffset(19), genOffset(30)),
    )

    // WHEN: completions older than cache are queried
    val pastCompletions = await(
      reader.getCompletionsPage(
        genOffset(9),
        genOffset(19),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )
    // THEN: correct number of completions should be returned
    pastCompletions.size shouldBe 10

    // AND database should be queried with application and parties filter
    completionsDao.runQueriesJournalToSeq() shouldBe Seq[Request](
      GetAllCompletionsRequest(genOffset(9), genOffset(19)),
      GetAllCompletionsRequest(genOffset(19), genOffset(30)),
      GetFilteredCompletionsRequest(
        genOffset(9),
        genOffset(19),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      ),
    )
  }

  it should "correctly fetch data overlapping with cache boundaries" in {

    // GIVEN: completions reader and dao mock with 40 completions
    val completionsDao = new CompletionsDaoMock(CompletionsDaoMock.genCompletions(40))
    val reader = new PagedCompletionsReaderWithCache(completionsDao, 10)

    // WHEN: not cached completions are fetched
    val cachedCompletions = await(
      reader.getCompletionsPage(
        genOffset(19),
        genOffset(29),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    // THEN: correct number of completions should be returned
    cachedCompletions.size shouldBe 10

    // WHEN completions for range bigger than cache size are requested
    val overlappingCompletions = await(
      reader.getCompletionsPage(
        genOffset(9),
        genOffset(39),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    // THEN: correct number of completions should be returned
    overlappingCompletions.size shouldBe 30

    // AND database should be queried with application and parties filter for older than cached completions (<19)
    // AND database should not be queried by cached completions (19, 29]
    // AND database should be queried for all completions newer than 29
    completionsDao.runQueriesJournalToSeq() shouldBe Seq[Request](
      GetAllCompletionsRequest(genOffset(19), genOffset(29)),
      GetFilteredCompletionsRequest(
        genOffset(9),
        genOffset(19),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      ),
      GetAllCompletionsRequest(genOffset(29), genOffset(39)),
    )
  }

  it should "fetch each completion exactly once" in {
    // GIVEN: completions reader and dao mock with 20 completions
    val completionsDao = new CompletionsDaoMock(CompletionsDaoMock.genCompletions(20))
    val reader = new PagedCompletionsReaderWithCache(completionsDao, 10)

    // WHEN 10 equal requests are performed concurrently
    val responses = await(Future.sequence((0 until 10).map { _ =>
      Future(
        reader.getCompletionsPage(
          genOffset(9),
          genOffset(19),
          ApplicationId.assertFromString("Application_1_Id"),
          Set(Party.assertFromString("party1")),
        )
      ).flatten
    }))
    responses.forall(_.size == 10) shouldBe true

    // THEN database should be queried only once
    completionsDao.runQueriesJournalToSeq() shouldBe Seq[Request](
      GetAllCompletionsRequest(genOffset(9), genOffset(19))
    )
  }

  private def await[T](f: Future[T]) = Await.result(f, Duration.apply(5, TimeUnit.SECONDS))

}
object CompletionsDaoMock {

  def genCompletions(
      startOffset: Int,
      endOffset: Int,
  ): ListBuffer[(Offset, CompletionStreamResponseWithParties)] =
    ListBuffer((startOffset until endOffset).map { i =>
      val resp = CompletionStreamResponseWithParties(
        completion = CompletionStreamResponse(
          checkpoint = None,
          completions = Seq(
            Completion(
              commandId = UUID.randomUUID().toString,
              status = None,
              transactionId = UUID.randomUUID().toString,
              traceContext = None,
            )
          ),
        ),
        parties = Set(Party.assertFromString("party1")),
        applicationId = ApplicationId.assertFromString("Application_1_Id"),
      )
      (genOffset(i), resp)
    }: _*)

  def genCompletions(endOffset: Int): ListBuffer[(Offset, CompletionStreamResponseWithParties)] =
    genCompletions(0, endOffset)

  sealed class Request

  final case class GetFilteredCompletionsRequest(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  ) extends Request

  final case class GetAllCompletionsRequest(
      startExclusive: Offset,
      endInclusive: Offset,
  ) extends Request
}

class CompletionsDaoMock(
    val completions: collection.mutable.ListBuffer[(Offset, CompletionStreamResponseWithParties)]
)(implicit ec: ExecutionContext)
    extends CompletionsDao {
  import CompletionsDaoMock._

  val runQueriesJournal: ConcurrentLinkedQueue[Request] = new ConcurrentLinkedQueue[Request]()

  def runQueriesJournalToSeq(): Seq[Request] =
    runQueriesJournal.toArray(new Array[Request](runQueriesJournal.size())).toSeq

  override def getFilteredCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[List[(Offset, CompletionStreamResponse)]] = {
    runQueriesJournal.add(
      GetFilteredCompletionsRequest(startExclusive, endInclusive, applicationId, parties)
    )
    Future(
      sortedCompletions()
        .filter { case (offset, completion) =>
          offset > startExclusive && offset <= endInclusive &&
            completion.applicationId == applicationId && completion.parties.exists(parties.contains)
        }
        .map { case (offset, completionWithParties) =>
          (offset, completionWithParties.completion)
        }
    )
  }

  override def getAllCompletions(startExclusive: Offset, endInclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Future[List[(Offset, CommandCompletionsTable.CompletionStreamResponseWithParties)]] = {
    runQueriesJournal.add(GetAllCompletionsRequest(startExclusive, endInclusive))
    Future(
      sortedCompletions().filter { case (offset, _) =>
        offset > startExclusive && offset <= endInclusive
      }
    )
  }

  private def sortedCompletions(): List[(Offset, CompletionStreamResponseWithParties)] =
    completions.sortBy(_._1).toList
}
