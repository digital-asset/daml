// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

case class ErrorCategoryAnnotations(
    description: Option[String],
    resolution: Option[String],
    retryStrategy: Option[String],
)

object ErrorCategoryAnnotations {
  def empty: ErrorCategoryAnnotations = ErrorCategoryAnnotations(None, None, None)
}
