// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.store.dao.CommandCompletionsTable.CompletionStreamResponseWithParties

import scala.concurrent.Future

/** data access object for completions
  */
trait CompletionsDao {

  /** Fetches all completions within the given boundaries that match the given application id
    * and are visible to at least one of the given parties.
    */
  def getFilteredCompletions(
      range: Range,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[List[(Offset, CompletionStreamResponse)]]

  /** Fetches all completions within the given range.
    */
  def getAllCompletions(range: Range)(implicit
      loggingContext: LoggingContext
  ): Future[List[(Offset, CompletionStreamResponseWithParties)]]
}
