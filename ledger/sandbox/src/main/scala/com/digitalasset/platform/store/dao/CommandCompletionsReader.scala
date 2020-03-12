// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.ApplicationId
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse

private[dao] object CommandCompletionsReader {

  private def offsetFor(response: CompletionStreamResponse): Offset =
    Offset.assertFromString(response.checkpoint.get.offset.get.getAbsolute)

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
