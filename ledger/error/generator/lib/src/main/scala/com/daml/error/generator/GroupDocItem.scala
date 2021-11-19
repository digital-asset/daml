// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import com.daml.error.{ErrorGroupPath, Explanation}

/** Contains error presentation data to be used for documentation rendering on the website.
  *
  * @param fullClassName The group class name (see [[com.daml.error.ErrorGroup]]).
  * @param explanation The detailed error explanation.
  * @param errorGroupPath Hierarchical grouping of this error group.
  */
case class GroupDocItem(
    fullClassName: String,
    explanation: Option[Explanation],
    errorGroupPath: ErrorGroupPath,
)
