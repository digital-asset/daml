// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data.keywords

import com.digitalasset.transcode.conformance.*
import com.digitalasset.transcode.schema.Descriptor.*
import com.digitalasset.transcode.schema.DynamicValue as DV

trait Enums extends TestCase:

  private val Enum = ctor(enumeration(Keywords))
  addCase(
    list(Enum),
    DV.List(Keywords.zipWithIndex.map((_, ix) => DV.Enumeration(ix))),
  )
