// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.object_meta

import com.daml.ledger.api.testtool.infrastructure.Assertions._

trait InvalidUpdateRequestsTests { self: ObjectMetaTests with ObjectMetaTestsBase =>

  testWithFreshResource(
    "FailingUpdateRequestsWhenUpdatePathIsDuplicated",
    "Failing an update request when an update path is duplicated",
  )()(implicit ec =>
    implicit ledger =>
      resource =>
        update(
          id = getId(resource),
          annotations = Map("k1" -> "v1"),
          updatePaths = Seq(annotationsUpdatePath, annotationsUpdatePath),
        ).mustFailWith(
          "updating a resource",
          invalidUpdateRequestErrorDescription(
            id = getId(resource),
            errorMessageSuffix = s"The update path: '$annotationsUpdatePath' is duplicated.",
          ),
        )
  )

  testWithFreshResource(
    "FailingUpdateRequestWhenNoUpdatePaths",
    "Failing an update request when the update mask is empty",
  )()(implicit ec =>
    implicit ledger =>
      resource =>
        update(
          id = getId(resource),
          annotations = Map("k1" -> "v1"),
          updatePaths = Seq.empty,
        )
          .mustFailWith(
            "updating a resource",
            invalidUpdateRequestErrorDescription(
              id = getId(resource),
              errorMessageSuffix = "The update mask contains no entries",
            ),
          )
  )

  testWithFreshResource(
    "FAilingUpdateRequestWhenUpdateMaskHaveUnknownFieldPath",
    "Failing an update request when the update mask contains a path to an unknown field",
  )()(implicit ec =>
    implicit ledger =>
      resource =>
        for {
          _ <- update(
            id = getId(resource),
            annotations = Map.empty,
            updatePaths = Seq("unknown_field"),
          ).mustFailWith(
            "fail 1",
            invalidUpdateRequestErrorDescription(
              id = getId(resource),
              errorMessageSuffix = "The update path: 'unknown_field' points to an unknown field.",
            ),
          )
          _ <- update(
            id = getId(resource),
            annotations = Map.empty,
            updatePaths = Seq("aaa!bbb"),
          ).mustFailWith(
            "fail 2",
            invalidUpdateRequestErrorDescription(
              id = getId(resource),
              errorMessageSuffix = "The update path: 'aaa!bbb' points to an unknown field.",
            ),
          )
          _ <- update(
            id = getId(resource),
            annotations = Map.empty,
            updatePaths = Seq(""),
          ).mustFailWith(
            "fail 3",
            invalidUpdateRequestErrorDescription(
              id = getId(resource),
              errorMessageSuffix = "The update path: '' points to an unknown field.",
            ),
          )
        } yield ()
  )

}
