package com.daml.platform.store.completions

import java.util.UUID
import java.util.concurrent.Executors

import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.Party
import com.daml.logging.LoggingContext
import com.daml.platform.store.dao.CommandCompletionsTable
import com.daml.platform.store.dao.CommandCompletionsTable.CompletionStreamResponseWithParties
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class PagedCompletionsReaderWithCacheSpec extends AnyFlatSpec with Matchers {
  import CompletionsDaoMock.genOffset

  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
  implicit val loggingContext = LoggingContext.ForTesting

  "foo" should "bla bla bla" in {

    val reader = new PagedCompletionsReaderWithCache(new CompletionsDaoMock, 30)

    val completions = Await.result(
      reader.getCompletionsPage(
        genOffset(0),
        genOffset(40),
        ApplicationId.assertFromString("Application_1_Id"),
        Set(Party.assertFromString("party1")),
      ),
      Duration.Inf,
    ) // TODO change to not inf

    completions.size shouldBe 20

    val completions2 = Await
      .result(
        reader.getCompletionsPage(
          genOffset(0),
          genOffset(40),
          ApplicationId.assertFromString("Application_1_Id"),
          Set(Party.assertFromString("party1")),
        ),
        Duration.Inf,
      )
    completions2.size shouldBe 20
  }

}
object CompletionsDaoMock {
  def genOffset(value: Int) = Offset.fromByteArray(Seq(value.toByte).toArray)
}

class CompletionsDaoMock(implicit ec: ExecutionContext) extends CompletionsDao {
  import CompletionsDaoMock._

  override def getFilteredCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[List[(Offset, CompletionStreamResponse)]] =
    Future(
      allCompletions.toList
        .filter { case (_, completion) =>
          completion.applicationId == applicationId && completion.parties.exists(parties.contains)
        }
        .map { case (offset, completionWithParties) =>
          (offset, completionWithParties.completion)
        }
    )

  override def getAllCompletions(startExclusive: Offset, endInclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Future[List[(Offset, CommandCompletionsTable.CompletionStreamResponseWithParties)]] = Future(
    allCompletions.toList
  )

  val allCompletions = (0 until 20).map { i =>
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
  }
}
