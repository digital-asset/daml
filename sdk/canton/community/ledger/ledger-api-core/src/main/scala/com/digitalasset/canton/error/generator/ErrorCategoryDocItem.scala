// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error.generator

final case class ErrorCategoryDocItem(
    description: Option[String],
    resolution: Option[String],
    retryStrategy: Option[String],
)

object ErrorCategoryDocItem {
  def empty: ErrorCategoryDocItem = ErrorCategoryDocItem(None, None, None)
}
