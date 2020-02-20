// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.ApplicationId
import com.digitalasset.ledger.api.domain.CompletionEvent
import com.digitalasset.platform.store.CompletionFromTransaction

private[dao] object CommandCompletionsReader {

  // Uses an existing LedgerReadDao to read completions from the ledger entries stream
  // TODO Replace this to tap directly into the index
  def apply(reader: LedgerReadDao): CommandCompletionsReader[LedgerDao#LedgerOffset] =
    (from: Long, to: Long, appId: ApplicationId, parties: Set[Ref.Party]) =>
      reader
        .getLedgerEntries(from, to)
        .map { case (offset, entry) => (offset + 1, entry) }
        .collect(CompletionFromTransaction(appId, parties))
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
    * @param startInclusive starting offset inclusive
    * @param endExclusive   ending offset exclusive
    * @return a stream of command completions tupled with their offset
    */
  def getCommandCompletions(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party]): Source[(LedgerOffset, CompletionEvent), NotUsed]

}
