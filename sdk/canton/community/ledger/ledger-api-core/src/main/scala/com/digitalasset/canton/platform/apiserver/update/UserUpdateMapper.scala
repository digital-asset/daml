// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.User
import com.digitalasset.canton.ledger.localstore.api.{ObjectMetaUpdate, UserUpdate}

object UserUpdateMapper extends UpdateMapperBase {

  import UpdateRequestsPaths.UserPaths

  type Resource = domain.User
  type Update = UserUpdate

  override val fullResourceTrie: UpdatePathsTrie = UserPaths.fullUpdateTrie

  override def makeUpdateObject(user: User, updateTrie: UpdatePathsTrie): Result[UserUpdate] = {
    for {
      annotationsUpdate <- resolveAnnotationsUpdate(updateTrie, user.metadata.annotations)
      primaryPartyUpdate <- resolvePrimaryPartyUpdate(updateTrie, user.primaryParty)
      isDeactivatedUpdate <- isDeactivatedUpdateResult(updateTrie, user.isDeactivated)
    } yield {
      UserUpdate(
        id = user.id,
        identityProviderId = user.identityProviderId,
        primaryPartyUpdateO = primaryPartyUpdate,
        isDeactivatedUpdateO = isDeactivatedUpdate,
        metadataUpdate = ObjectMetaUpdate(
          resourceVersionO = user.metadata.resourceVersionO,
          annotationsUpdateO = annotationsUpdate,
        ),
      )
    }
  }

  def resolveAnnotationsUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Map[String, String],
  ): Result[Option[Map[String, String]]] =
    updateTrie
      .findMatch(UserPaths.annotations)
      .fold(noUpdate[Map[String, String]])(updateMatch =>
        makeAnnotationsUpdate(newValue = newValue, updateMatch = updateMatch)
      )

  def resolvePrimaryPartyUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Option[Ref.Party],
  ): Result[Option[Option[Ref.Party]]] =
    updateTrie
      .findMatch(UserPaths.primaryParty)
      .fold(noUpdate[Option[Ref.Party]])(updateMatch =>
        makePrimitiveFieldUpdate(
          updateMatch = updateMatch,
          defaultValue = None,
          newValue = newValue,
        )
      )

  def isDeactivatedUpdateResult(
      updateTrie: UpdatePathsTrie,
      newValue: Boolean,
  ): Result[Option[Boolean]] =
    updateTrie
      .findMatch(UserPaths.isDeactivated)
      .fold(noUpdate[Boolean])(matchResult =>
        makePrimitiveFieldUpdate(
          updateMatch = matchResult,
          defaultValue = false,
          newValue = newValue,
        )
      )

}
