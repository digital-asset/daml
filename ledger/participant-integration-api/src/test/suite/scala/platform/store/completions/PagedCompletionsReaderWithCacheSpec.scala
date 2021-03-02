package com.daml.platform.store.completions

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}

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

class PagedCompletionsReaderWithCacheSpec extends AnyFlatSpec with Matchers {
  import CompletionsDaoMock.genOffset

  implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
  implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private def await[T](f: Future[T]) = Await.result(f, Duration.Inf)

  it should "correctly fetch historic data" in {

    val completionsDao = new CompletionsDaoMock(CompletionsDaoMock.genCompletions(20))

    val reader = new PagedCompletionsReaderWithCache(completionsDao, 10)

    val cachedCompletions = await(
      reader.getCompletionsPage(
        genOffset(9),
        genOffset(19),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    cachedCompletions.size shouldBe 10

    completionsDao.runQueriesJournalToSeq() shouldBe Seq(
      GetAllCompletionsRequest(genOffset(9), genOffset(19))
    )

    val historicCompletions = await(
      reader.getCompletionsPage(
        Offset.beforeBegin,
        genOffset(9),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    historicCompletions.size shouldBe 10

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

    val completionsDao = new CompletionsDaoMock(CompletionsDaoMock.genCompletions(20))

    val reader = new PagedCompletionsReaderWithCache(completionsDao, 10)

    val cachedCompletions = await(
      reader.getCompletionsPage(
        genOffset(9),
        genOffset(19),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    cachedCompletions.size shouldBe 10

    completionsDao.runQueriesJournalToSeq() shouldBe Seq(
      GetAllCompletionsRequest(genOffset(9), genOffset(19))
    )

    completionsDao.completions ++= CompletionsDaoMock.genCompletions(20, 30)

    val futureCompletions = await(
      reader.getCompletionsPage(
        genOffset(9),
        genOffset(30),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    futureCompletions.size shouldBe 20

    completionsDao.runQueriesJournalToSeq() shouldBe Seq[Request](
      GetAllCompletionsRequest(genOffset(9), genOffset(19)),
      GetAllCompletionsRequest(genOffset(19), genOffset(30)),
    )

    val pastCompletions = await(
      reader.getCompletionsPage(
        genOffset(9),
        genOffset(19),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    pastCompletions.size shouldBe 10

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
    val completionsDao = new CompletionsDaoMock(CompletionsDaoMock.genCompletions(40))

    val reader = new PagedCompletionsReaderWithCache(completionsDao, 10)

    val cachedCompletions = await(
      reader.getCompletionsPage(
        genOffset(19),
        genOffset(29),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    cachedCompletions.size shouldBe 10

    val overlappingCompletions = await(
      reader.getCompletionsPage(
        genOffset(9),
        genOffset(39),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      )
    )

    overlappingCompletions.size shouldBe 30

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
    val completionsDao = new CompletionsDaoMock(CompletionsDaoMock.genCompletions(20))
    val reader = new PagedCompletionsReaderWithCache(completionsDao, 10)

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

    completionsDao.runQueriesJournalToSeq() shouldBe Seq[Request](
      GetAllCompletionsRequest(genOffset(9), genOffset(19))
    )
  }

}
object CompletionsDaoMock {
  def genOffset(value: Int): Offset = Offset.fromByteArray(Seq(value.toByte).toArray)

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
