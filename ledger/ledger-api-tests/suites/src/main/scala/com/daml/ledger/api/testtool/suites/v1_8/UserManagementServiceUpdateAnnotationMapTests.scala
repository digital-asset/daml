// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Assertions._

trait UserManagementServiceUpdateAnnotationMapTests {
  self: UserManagementServiceUpdateRpcTests with UserManagementServiceIT =>

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithNonEmptyMapAndExactUpdatePath",
    "Update annotations using update paths with a non-empty map and the exact update path",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k1" -> "v1a", "k3" -> "", "k4" -> "v4", "k5" -> ""),
          updatePaths = Seq("metadata.annotations"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations",
          extractUpdatedAnnotations(updateResp),
          Map("k1" -> "v1a", "k2" -> "v2", "k4" -> "v4"),
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithNonEmptyMapAndShorthandUpdatePath",
    "Update annotations using update paths with a non-empty map and a shorthand update path",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k1" -> "v1a", "k3" -> "", "k4" -> "v4"),
          updatePaths = Seq("metadata"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations",
          extractUpdatedAnnotations(updateResp),
          Map("k1" -> "v1a", "k2" -> "v2", "k4" -> "v4"),
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithEmptyMapAndExactUpdatePath",
    "Update annotations using update paths with the empty map and the the exact update path",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map.empty,
          updatePaths = Seq("metadata.annotations"),
        )
      )
      .map { responseResp =>
        assertEquals(
          "updating annotations",
          extractUpdatedAnnotations(responseResp),
          Map("k1" -> "v1", "k2" -> "v2"),
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithEmptyMapAndShorthandUpdatePath",
    "Update annotations using update paths with the empty map and a shorthand update path",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map.empty,
          updatePaths = Seq("metadata"),
        )
      )
      .map { responseResp =>
        assertEquals(
          "updating annotations",
          extractUpdatedAnnotations(responseResp),
          Map("k1" -> "v1", "k2" -> "v2"),
        )
      }
  })

}
