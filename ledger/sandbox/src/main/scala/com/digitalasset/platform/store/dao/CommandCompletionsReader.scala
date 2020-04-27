// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.platform.ApiOffset

private[dao] object CommandCompletionsReader {

  private def offsetFor(response: CompletionStreamResponse): Offset =
    ApiOffset.assertFromString(response.checkpoint.get.offset.get.getAbsolute)

  def apply(dispatcher: DbDispatcher): CommandCompletionsReader[Offset] =
    (from: Offset, to: Offset, appId: ApplicationId, parties: Set[Ref.Party]) => {
      val query = CommandCompletionsTable.prepareGet(from, to, appId, parties)
      Source
        .future(dispatcher.executeSql("get_completions") { implicit connection =>
          query.as(CommandCompletionsTable.parser.*)
        })
        .mapConcat(_.map(response => offsetFor(response) -> response))
    }
}

trait CommandCompletionsReader[LedgerOffset] {

  /**
    * Returns a stream of command completions
    *
    * TODO The current type parameter is to differentiate between checkpoints
    * TODO and actual completions, it will change when we drop checkpoints
    *
    * TODO Drop the LedgerOffset from the source when we replace the Dispatcher mechanism
    *
    * @return a stream of command completions tupled with their offset
    */
  def getCommandCompletions(
      startExclusive: LedgerOffset,
      endInclusive: LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party]): Source[(LedgerOffset, CompletionStreamResponse), NotUsed]

}
