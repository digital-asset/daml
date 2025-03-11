// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error.generator

import com.digitalasset.base.error.{ErrorClass, Explanation, Resolution}
import com.digitalasset.canton.error.generator.ErrorCodeDocumentationGenerator.DeprecatedItem

/** Contains error presentation data to be used for documentation rendering on the website.
  *
  * @param errorCodeClassName
  *   The error class name (see [[com.digitalasset.base.error.ErrorCode]]).
  * @param category
  *   The error code category (see [[com.digitalasset.base.error.ErrorCategory]]).
  * @param hierarchicalGrouping
  *   The hierarchical code grouping (see [[com.digitalasset.base.error.ErrorClass]] and
  *   [[com.digitalasset.base.error.ErrorGroup]]).
  * @param conveyance
  *   Provides a statement about the form this error will be returned to the user.
  * @param code
  *   The error identifier.
  * @param explanation
  *   The detailed error explanation.
  * @param resolution
  *   The suggested error resolution.
  */
final case class ErrorCodeDocItem(
    errorCodeClassName: String,
    category: String,
    hierarchicalGrouping: ErrorClass,
    conveyance: Option[String],
    code: String,
    deprecation: Option[DeprecatedItem],
    explanation: Option[Explanation],
    resolution: Option[Resolution],
)
