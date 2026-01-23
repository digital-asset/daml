// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data

import com.digitalasset.transcode.conformance.*
import com.digitalasset.transcode.schema.Descriptor.enumeration
import com.digitalasset.transcode.schema.DynamicValue

trait Enums extends TestCase:
  private val Foo = ctor(enumeration("Bar", "Baz", "Qux"))
  addCase(
    Foo,
    DynamicValue.Enumeration(0),
    DynamicValue.Enumeration(1),
    DynamicValue.Enumeration(2),
  )
  addFailingCase(
    Foo,
    "Unexpected case" -> DynamicValue.Enumeration(3),
  )

  private val This = ctor(enumeration("This", "That"))
  addCase(
    This,
    DynamicValue.Enumeration(0),
  )
