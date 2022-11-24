// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{IdentityProviderId, User}
import com.daml.lf.data.Ref
import com.daml.platform.localstore.api.{ObjectMetaUpdate, UserUpdate}

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
      isIdentityProviderIdUpdate <- isIdentityProviderIdUpdate(updateTrie, user.identityProviderId)
    } yield {
      UserUpdate(
        id = user.id,
        primaryPartyUpdateO = primaryPartyUpdate,
        isDeactivatedUpdateO = isDeactivatedUpdate,
        metadataUpdate = ObjectMetaUpdate(
          resourceVersionO = user.metadata.resourceVersionO,
          annotationsUpdateO = annotationsUpdate,
        ),
        identityProviderIdUpdate = isIdentityProviderIdUpdate,
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

  def isIdentityProviderIdUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: IdentityProviderId,
  ): Result[Option[IdentityProviderId]] =
    updateTrie
      .findMatch(UserPaths.identityProviderId)
      .fold(noUpdate[IdentityProviderId])(matchResult =>
        makePrimitiveFieldUpdate(
          updateMatch = matchResult,
          defaultValue = IdentityProviderId.Default,
          newValue = newValue,
        )
      )

}
