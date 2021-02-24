package com.daml.platform.store.completions

import com.daml.ledger.ApplicationId
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.store.dao.CommandCompletionsTable.CompletionStreamResponseWithParties

import scala.concurrent.Future

trait CompletionsDao {
  def getFilteredCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[List[(Offset, CompletionStreamResponseWithParties)]]

  def getAllCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit
      loggingContext: LoggingContext
  ): Future[List[(Offset, CompletionStreamResponseWithParties)]]
}
