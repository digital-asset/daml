// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.store.completions.{CompletionsDao, PagedCompletionsReader}
import com.daml.platform.store.completions.Range

import scala.concurrent.Future

private[dao] final class CommandCompletionsReader(
    completionsDao: CompletionsDao
) extends PagedCompletionsReader {

  override def getCompletionsPage(
      range: Range,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Seq[(Offset, CompletionStreamResponse)]] =
    completionsDao.getFilteredCompletions(range, applicationId, parties)
}
