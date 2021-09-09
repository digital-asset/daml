// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import com.daml.error.{Explanation, Resolution}

/** Contains error presentation data to be used for documentation rendering on the website.
  *
  * @param className The error class name (see [[com.daml.error.ErrorCode]]).
  * @param category The error code category (see [[com.daml.error.ErrorCategory]]).
  * @param hierarchicalGrouping The hierarchical code grouping
  *                             (see [[com.daml.error.ErrorClass]] and [[com.daml.error.ErrorGroup]]).
  * @param conveyance Provides a statement about the form this error will be returned to the user.
  * @param code The error identifier.
  * @param explanation The detailed error explanation.
  * @param resolution The suggested error resolution.
  */
case class DocItem(
    className: String,
    category: String,
    hierarchicalGrouping: List[String],
    conveyance: String,
    code: String,
    explanation: Explanation,
    resolution: Resolution,
)
