// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.proto

import com.digitalasset.transcode.utils.codecspec.CodecCommonSpec
import zio.*
import zio.test.*

object ProtobufCodecSpec extends CodecCommonSpec[Value]:
  override val schemaProcessor = ProtobufCodec()

  override def spec =
    suite("ProtobufCodec (Java)")(autoTests)
