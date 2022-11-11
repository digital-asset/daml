// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

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

}
