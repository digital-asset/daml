// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

import com.digitalasset.canton.ledger.api.domain.JwksUrl
import com.digitalasset.canton.ledger.localstore.api.IdentityProviderConfigUpdate

object IdentityProviderConfigUpdateMapper extends UpdateMapperBase {

  import UpdateRequestsPaths.IdentityProviderConfigPaths

  type Resource = IdentityProviderConfigUpdate
  type Update = IdentityProviderConfigUpdate

  override val fullResourceTrie: UpdatePathsTrie = IdentityProviderConfigPaths.fullUpdateTrie

  override def makeUpdateObject(
      identityProviderConfig: IdentityProviderConfigUpdate,
      updateTrie: UpdatePathsTrie,
  ): Result[IdentityProviderConfigUpdate] = {
    for {
      isDeactivatedUpdate <- resolveIsDeactivatedUpdate(
        updateTrie,
        identityProviderConfig.isDeactivatedUpdate,
      )
      issuerUpdate <- resolveIssuerUpdate(updateTrie, identityProviderConfig.issuerUpdate)
      jwksUrlUpdate <- resolveJwksUrlUpdate(updateTrie, identityProviderConfig.jwksUrlUpdate)
      audienceUpdate <- resolveAudienceUpdate(
        updateTrie,
        identityProviderConfig.audienceUpdate,
      )
    } yield {
      IdentityProviderConfigUpdate(
        identityProviderId = identityProviderConfig.identityProviderId,
        isDeactivatedUpdate = isDeactivatedUpdate,
        jwksUrlUpdate = jwksUrlUpdate,
        issuerUpdate = issuerUpdate,
        audienceUpdate = audienceUpdate,
      )
    }
  }

  def resolveAudienceUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Option[Option[String]],
  ): Result[Option[Option[String]]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.audience)
      .fold(noUpdate[Option[String]])(updateMatch =>
        makePrimitiveFieldUpdate[Option[String]](
          updateMatch = updateMatch,
          defaultValue = None,
          newValue = newValue.flatten,
        )
      )

  def resolveIsDeactivatedUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Option[Boolean],
  ): Result[Option[Boolean]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.isDeactivated)
      .fold(noUpdate[Boolean])(updateMatch =>
        if (updateMatch.isExact) {
          Right(newValue)
        } else {
          Right(None)
        }
      )

  def resolveIssuerUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Option[String],
  ): Result[Option[String]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.issuer)
      .fold(noUpdate[String])(updateMatch =>
        if (updateMatch.isExact) {
          Right(newValue)
        } else {
          Right(None)
        }
      )

  def resolveJwksUrlUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Option[JwksUrl],
  ): Result[Option[JwksUrl]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.jwksUrl)
      .fold(noUpdate[JwksUrl])(updateMatch =>
        if (updateMatch.isExact) {
          Right(newValue)
        } else {
          Right(None)
        }
      )

}
