// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset

private[dao] final class CommandCompletionsReader(dispatcher: DbDispatcher, metrics: Metrics) {

  private def offsetFor(response: CompletionStreamResponse): Offset =
    ApiOffset.assertFromString(response.checkpoint.get.offset.get.getAbsolute)

  def getCommandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party]): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    val query = CommandCompletionsTable.prepareGet(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      applicationId = applicationId,
      parties = parties,
    )
    Source
      .future(dispatcher.executeSql(metrics.daml.index.db.getCompletions) { implicit connection =>
        query.as(CommandCompletionsTable.parser.*)
      })
      .mapConcat(_.map(response => offsetFor(response) -> response))
  }

}
