// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data.keywords

import com.digitalasset.transcode.conformance.*
import com.digitalasset.transcode.schema.Descriptor.*
import com.digitalasset.transcode.schema.DynamicValue as DV

trait Variants extends TestCase:

  private val Variant = ctor(
    variant(
      Keywords.map(name => name -> unit)
        :+ ("Dummy" -> text) // Dummy case to ensure daml doesn't treat it as enum
    )
  )
  addCase(
    list(Variant),
    DV.List(Keywords.zipWithIndex.map((_, ix) => DV.Variant(ix, DV.Unit))),
  )
