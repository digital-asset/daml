// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.platform.localstore.api.IdentityProviderConfigUpdate

import java.net.URL

object IdentityProviderConfigUpdateMapper extends UpdateMapperBase {

  import UpdateRequestsPaths.IdentityProviderConfigPaths

  type Resource = IdentityProviderConfig
  type Update = IdentityProviderConfigUpdate

  override val fullResourceTrie: UpdatePathsTrie = IdentityProviderConfigPaths.fullUpdateTrie

  override def makeUpdateObject(
      identityProviderConfig: IdentityProviderConfig,
      updateTrie: UpdatePathsTrie,
  ): Result[IdentityProviderConfigUpdate] = {
    for {
      isDeactivatedUpdate <- resolveIsDeactivatedUpdate(
        updateTrie,
        identityProviderConfig.isDeactivated,
      )
      issuerUpdate <- resolveIssuerUpdate(updateTrie, identityProviderConfig.issuer)
      jwksUrlUpdate <- resolveJwksUrlUpdate(updateTrie, identityProviderConfig.jwksURL)
    } yield {
      IdentityProviderConfigUpdate(
        identityProviderId = identityProviderConfig.identityProviderId,
        isDeactivatedUpdate = isDeactivatedUpdate,
        jwksUrlUpdate = jwksUrlUpdate,
        issuerUpdate = issuerUpdate,
      )
    }
  }

  def resolveIsDeactivatedUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Boolean,
  ): Result[Option[Boolean]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.isDeactivated)
      .fold(noUpdate[Boolean])(updateMatch =>
        makePrimitiveFieldUpdate(
          updateMatch = updateMatch,
          defaultValue = false,
          newValue = newValue,
        )
      )

  def resolveIssuerUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: String,
  ): Result[Option[String]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.issuer)
      .fold(noUpdate[String])(updateMatch =>
        makePrimitiveFieldUpdate(
          updateMatch = updateMatch,
          defaultValue = "",
          newValue = newValue,
        )
      )

  def resolveJwksUrlUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: URL,
  ): Result[Option[URL]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.jwksUrl)
      .fold(noUpdate[URL])(updateMatch =>
        if (updateMatch.isExact) {
          Right(Some(newValue))
        } else {
          Right(None)
        }
      )

}
