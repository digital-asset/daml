package com.daml.platform.store.completions

import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

trait PagedCompletionsReader {

  /** Returns completions stream filtered based on parameters.
    * Newest completions read from database are added to cache
    * @param startExclusive
    * @param endInclusive
    * @param applicationId
    * @param parties
    * @param loggingContext
    * @param executionContext
    * @return
    */
  def getCompletionsPage(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Future[Seq[(Offset, CompletionStreamResponse)]]
}
