// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.object_meta

import java.nio.charset.StandardCharsets

import com.daml.ledger.api.testtool.infrastructure.Assertions._

trait AnnotationsTests
    extends ResourceCreationAnnotationsTests
    with ResourceUpdateAnnotationsTests { self: ObjectMetaTests with ObjectMetaTestsBase =>

  private[object_meta] def maxAnnotationsSizeInBytes = 256 * 1024
  private[object_meta] def valueExceedingAnnotationsLimit = "a" * maxAnnotationsSizeInBytes
  private[object_meta] def largestAllowedValue = "a" * (maxAnnotationsSizeInBytes - 1)
  private[object_meta] def annotationsOverSizeLimit = Map("a" -> largestAllowedValue, "c" -> "d")
  private[object_meta] def annotationsBelowMaxSizeLimitBecauseNotCountingEmptyValuedKeys =
    Map("a" -> largestAllowedValue, "cc" -> "")

  private[object_meta] def getAnnotationsBytes(annotations: Map[String, String]): Int =
    annotations.iterator.map { case (k, v) =>
      k.getBytes(StandardCharsets.UTF_8).length + v.getBytes(StandardCharsets.UTF_8).length
    }.sum

  assertEquals(
    valueExceedingAnnotationsLimit.getBytes(StandardCharsets.UTF_8).length,
    maxAnnotationsSizeInBytes,
  )
  assertEquals(
    getAnnotationsBytes(annotationsOverSizeLimit),
    getAnnotationsBytes(annotationsBelowMaxSizeLimitBecauseNotCountingEmptyValuedKeys),
  )

  private[object_meta] def invalidKey = ".aaaa.management.daml/foo_"
  private[object_meta] def validKey = "0-aaaa.management.daml/foo"

}
