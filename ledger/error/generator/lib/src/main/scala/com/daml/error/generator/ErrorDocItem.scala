// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import com.daml.error.{Deprecation, ErrorGroupPath, Explanation, Resolution}

/** Contains error presentation data to be used for documentation rendering on the website.
  *
  * @param fullClassName The error class name (see [[com.daml.error.ErrorCode]]).
  * @param category The error code category (see [[com.daml.error.ErrorCategory]]).
  * @param errorGroupPath The hierarchical code grouping
  *                             (see [[com.daml.error.ErrorGroupPath]] and [[com.daml.error.ErrorGroup]]).
  * @param conveyance Provides a statement about the form this error will be returned to the user.
  * @param code The error identifier.
  * @param explanation The detailed error explanation.
  * @param resolution The suggested error resolution.
  */
case class ErrorDocItem(
    fullClassName: String, // TODO error codes: Rename to `errorCodeName` or `errorCodeClassName` to prevent confusion
    category: String,
    errorGroupPath: ErrorGroupPath,
    conveyance: Option[String],
    code: String,
    deprecation: Option[Deprecation],
    explanation: Option[Explanation],
    resolution: Option[Resolution],
)
