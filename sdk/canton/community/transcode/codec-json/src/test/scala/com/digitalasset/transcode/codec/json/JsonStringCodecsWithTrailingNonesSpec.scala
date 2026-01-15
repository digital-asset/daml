// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.json

import com.digitalasset.transcode.utils.codecspec.CodecCommonSpec

object JsonStringCodecsWithTrailingNonesSpec extends CodecCommonSpec[String] {
  override val schemaProcessor = JsonStringCodec(removeTrailingNonesInRecords = false)

  override def spec = suite("JsonStringCodecsWithTrailingNonesSpec")(autoTests)
}
