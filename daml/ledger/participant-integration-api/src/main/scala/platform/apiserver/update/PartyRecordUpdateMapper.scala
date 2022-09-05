// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.ParticipantParty
import com.daml.ledger.participant.state.index.v2.{
  AnnotationsUpdate,
  ObjectMetaUpdate,
  PartyRecordUpdate,
}

object PartyRecordUpdateMapper extends UpdateMapperBase {

  import UpdateRequestsPaths.PartyDetailsPaths

  type DomainObject = domain.ParticipantParty.PartyRecord
  type UpdateObject = PartyRecordUpdate

  override val fullUpdateTrie: UpdatePathsTrie = PartyDetailsPaths.fullUpdateTrie

  override def makeUpdateObject(
      partyRecord: ParticipantParty.PartyRecord,
      updateTrie: UpdatePathsTrie,
  ): Result[PartyRecordUpdate] = {
    for {
      annotationsUpdate <- resolveAnnotationsUpdate(updateTrie, partyRecord.metadata.annotations)
    } yield {
      PartyRecordUpdate(
        party = partyRecord.party,
        metadataUpdate = ObjectMetaUpdate(
          resourceVersionO = partyRecord.metadata.resourceVersionO,
          annotationsUpdateO = annotationsUpdate,
        ),
      )
    }
  }

  def resolveAnnotationsUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Map[String, String],
  ): Result[Option[AnnotationsUpdate]] =
    updateTrie
      .findMatch(PartyDetailsPaths.annotations)
      .fold(noUpdate[AnnotationsUpdate])(updateMatch =>
        makeAnnotationsUpdate(
          newValue = newValue,
          updateMatch = updateMatch,
        )
      )

}
