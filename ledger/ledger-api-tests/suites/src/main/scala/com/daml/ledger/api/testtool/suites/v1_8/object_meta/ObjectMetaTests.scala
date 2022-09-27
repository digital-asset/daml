// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.object_meta

import com.daml.ledger.api.testtool.infrastructure.Assertions._

// Gather all the tests common to resources that support ObjectMeta metadata
trait ObjectMetaTests
    extends AnnotationsTests
    with ConcurrentChangeControlTests
    with InvalidUpdateRequestsTests { self: ObjectMetaTestsBase =>

  testWithFreshResource(
    "AllowSpecifyingResourceVersionAndResourceIdInUpdateMask",
    "Allow specifying resource_version and resource's id fields in the update mask",
  )()(implicit ec =>
    implicit ledger =>
      resource =>
        update(
          id = getId(resource),
          annotations = Map.empty,
          resourceVersion = "",
          updatePaths = Seq(resourceIdPath, resourceVersionUpdatePath),
        ).map { objectMeta =>
          assertEquals(
            objectMeta.annotations,
            extractAnnotations(resource),
          )
        }
  )

}
