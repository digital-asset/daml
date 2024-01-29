// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import java.nio.charset.StandardCharsets

object ResourceAnnotationValidation {

  // NOTE: These constraints are based on constraints K8s uses for their annotations and labels
  val MaxAnnotationsSizeInKiloBytes: Int = 256
  private val MaxAnnotationsSizeInBytes: Int = MaxAnnotationsSizeInKiloBytes * 1024

  /** @return a Left(actualSizeInBytes) in case of a failed validation
    */
  def isWithinMaxAnnotationsByteSize(annotations: Map[String, String]): Boolean = {
    val totalSizeInBytes = annotations.iterator.foldLeft(0L) { case (size, (key, value)) =>
      val keySize = key.getBytes(StandardCharsets.UTF_8).length
      val valSize = value.getBytes(StandardCharsets.UTF_8).length
      size + keySize + valSize
    }
    totalSizeInBytes <= MaxAnnotationsSizeInBytes
  }
}
