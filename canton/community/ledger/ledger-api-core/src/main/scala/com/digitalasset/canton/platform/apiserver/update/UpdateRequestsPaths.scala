// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

protected[update] object UpdateRequestsPaths {

  object UserPaths {
    val id: List[String] = List(FieldNames.User.id)
    val annotations: List[String] = List(FieldNames.User.metadata, FieldNames.Metadata.annotations)
    val resourceVersion: List[String] =
      List(FieldNames.User.metadata, FieldNames.Metadata.resourceVersion)
    val primaryParty: List[String] = List(FieldNames.User.primaryParty)
    val isDeactivated: List[String] = List(FieldNames.User.isDeactivated)
    val identityProviderId: List[String] = List(FieldNames.User.identityProviderId)
    val fullUpdateTrie: UpdatePathsTrie = UpdatePathsTrie
      .fromPaths(
        Seq(
          id,
          primaryParty,
          isDeactivated,
          annotations,
          resourceVersion,
          identityProviderId,
        )
      )
      .getOrElse(sys.error("Failed to create full update user tree. This should never happen"))
  }

  object PartyDetailsPaths {
    val party: List[String] = List(FieldNames.PartyDetails.party)
    val annotations: List[String] =
      List(FieldNames.PartyDetails.localMetadata, FieldNames.Metadata.annotations)
    val resourceVersion: List[String] =
      List(FieldNames.PartyDetails.localMetadata, FieldNames.Metadata.resourceVersion)
    val displayName: List[String] = List(FieldNames.PartyDetails.displayName)
    val isLocal: List[String] = List(FieldNames.PartyDetails.isLocal)
    val identityProviderId: List[String] = List(FieldNames.PartyDetails.identityProviderId)
    val fullUpdateTrie: UpdatePathsTrie = UpdatePathsTrie
      .fromPaths(
        Seq(
          party,
          displayName,
          isLocal,
          annotations,
          resourceVersion,
          identityProviderId,
        )
      )
      .getOrElse(sys.error("Failed to create full update user tree. This should never happen"))
  }

  object IdentityProviderConfigPaths {
    val identityProviderId: List[String] = List(
      FieldNames.IdentityProviderConfig.identityProviderId
    )
    val isDeactivated: List[String] = List(FieldNames.IdentityProviderConfig.isDeactivated)
    val jwksUrl: List[String] = List(FieldNames.IdentityProviderConfig.jwksUrl)
    val issuer: List[String] = List(FieldNames.IdentityProviderConfig.issuer)
    val audience: List[String] = List(FieldNames.IdentityProviderConfig.audience)
    val fullUpdateTrie: UpdatePathsTrie = UpdatePathsTrie
      .fromPaths(
        Seq(
          identityProviderId,
          isDeactivated,
          jwksUrl,
          issuer,
          audience,
        )
      )
      .getOrElse(
        sys.error(
          "Failed to create full update identity provider config tree. This should never happen"
        )
      )
  }
}
