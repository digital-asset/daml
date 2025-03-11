// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error.generator

import com.digitalasset.base.error.{ErrorClass, Explanation}

/** Contains error presentation data to be used for documentation rendering on the website.
  *
  * @param className
  *   The group class name (see [[com.digitalasset.base.error.ErrorGroup]]).
  * @param explanation
  *   The detailed error explanation.
  * @param errorClass
  *   Hierarchical grouping of this error group.
  */
final case class ErrorGroupDocItem(
    className: String,
    explanation: Option[Explanation],
    errorClass: ErrorClass,
)
