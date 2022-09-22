// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

protected[update] object UpdateRequestsPaths {

  object UserPaths {
    val annotations: List[String] = List(
      FieldNames.UpdateUserRequest.user,
      FieldNames.User.metadata,
      FieldNames.Metadata.annotations,
    )
    val primaryParty: List[String] =
      List(FieldNames.UpdateUserRequest.user, FieldNames.User.primaryParty)
    val isDeactivated =
      List(FieldNames.UpdateUserRequest.user, FieldNames.User.isDeactivated)
    val fullUpdateTrie: UpdatePathsTrie = UpdatePathsTrie
      .fromPaths(
        Seq(
          annotations,
          primaryParty,
          isDeactivated,
        )
      )
      .getOrElse(sys.error("Failed to create full update user tree. This should never happen"))
  }

  object PartyDetailsPaths {
    val annotations: List[String] = List(
      FieldNames.UpdatePartyDetailsRequest.partyDetails,
      FieldNames.PartyDetails.localMetadata,
      FieldNames.Metadata.annotations,
    )
    val fullUpdateTrie: UpdatePathsTrie = UpdatePathsTrie
      .fromPaths(Seq(annotations))
      .getOrElse(sys.error("Failed to create full update user tree. This should never happen"))
  }

}
