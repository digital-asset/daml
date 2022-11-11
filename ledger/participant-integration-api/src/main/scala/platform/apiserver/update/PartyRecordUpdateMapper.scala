// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import com.daml.ledger.api.domain.PartyDetails
import com.daml.lf.data.Ref
import com.daml.platform.localstore.api.{ObjectMetaUpdate, PartyDetailsUpdate}

object PartyRecordUpdateMapper extends UpdateMapperBase {

  import UpdateRequestsPaths.PartyDetailsPaths

  type Resource = PartyDetails
  type Update = PartyDetailsUpdate

  override val fullResourceTrie: UpdatePathsTrie = PartyDetailsPaths.fullUpdateTrie

  override def makeUpdateObject(
      partyRecord: PartyDetails,
      updateTrie: UpdatePathsTrie,
  ): Result[PartyDetailsUpdate] = {
    for {
      annotationsUpdate <- resolveAnnotationsUpdate(updateTrie, partyRecord.metadata.annotations)
      isLocalUpdate <- resolveIsLocalUpdate(updateTrie, partyRecord.isLocal)
      displayNameUpdate <- resolveDisplayNameUpdate(updateTrie, partyRecord.displayName)
      identityProviderIdUpdate <- resolveIdentityProviderIdUpdate(
        updateTrie,
        partyRecord.identityProviderId,
      )
    } yield {
      PartyDetailsUpdate(
        party = partyRecord.party,
        displayNameUpdate = displayNameUpdate,
        isLocalUpdate = isLocalUpdate,
        metadataUpdate = ObjectMetaUpdate(
          resourceVersionO = partyRecord.metadata.resourceVersionO,
          annotationsUpdateO = annotationsUpdate,
        ),
        identityProviderIdUpdate = identityProviderIdUpdate,
      )
    }
  }

  def resolveDisplayNameUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Option[String],
  ): Result[Option[Option[String]]] =
    updateTrie
      .findMatch(PartyDetailsPaths.displayName)
      .fold(noUpdate[Option[String]])(updateMatch =>
        makePrimitiveFieldUpdate(
          updateMatch = updateMatch,
          defaultValue = None,
          newValue = newValue,
        )
      )

  def resolveIsLocalUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Boolean,
  ): Result[Option[Boolean]] =
    updateTrie
      .findMatch(PartyDetailsPaths.isLocal)
      .fold(noUpdate[Boolean])(updateMatch =>
        makePrimitiveFieldUpdate(
          updateMatch = updateMatch,
          defaultValue = false,
          newValue = newValue,
        )
      )

  def resolveIdentityProviderIdUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Ref.IdentityProviderId,
  ): Result[Option[Ref.IdentityProviderId]] =
    updateTrie
      .findMatch(PartyDetailsPaths.identityProviderId)
      .fold(noUpdate[Ref.IdentityProviderId])(matchResult =>
        makePrimitiveFieldUpdate(
          updateMatch = matchResult,
          defaultValue = Ref.IdentityProviderId.Default,
          newValue = newValue,
        )
      )

  def resolveAnnotationsUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Map[String, String],
  ): Result[Option[Map[String, String]]] =
    updateTrie
      .findMatch(PartyDetailsPaths.annotations)
      .fold(noUpdate[Map[String, String]])(updateMatch =>
        makeAnnotationsUpdate(newValue = newValue, updateMatch = updateMatch)
      )

}
