// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data.keywords

import com.digitalasset.transcode.conformance.*
import com.digitalasset.transcode.schema.Descriptor.*
import com.digitalasset.transcode.schema.DynamicValue as DV

trait RecordFields extends TestCase:

  private val Record = ctor(record(keywords.map(k => k -> unit)))
  addCase(Record, DV.Record(keywords.map(_ => DV.Unit)))
