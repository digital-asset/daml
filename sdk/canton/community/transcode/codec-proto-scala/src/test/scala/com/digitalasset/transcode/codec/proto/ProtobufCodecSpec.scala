// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.proto

import com.daml.ledger.api.v2.value
import com.digitalasset.transcode.schema.*

import Value.Sum

object ProtobufCodecSpec extends ProtobufCodecSpecDefault:
  override def textMapSuite =
    suite("textMap")(
      testCodec(
        "empty",
        Value(Sum.TextMap(value.TextMap())),
        DynamicValue.TextMap(Seq.empty),
        schemaProcessor.textMap(schemaProcessor.unit),
      ),
      testCodec(
        "non-empty",
        Value(
          Sum.TextMap(
            value.TextMap(
              Seq(
                value.TextMap.Entry("entry1", Some(Value(Sum.Text("some text1")))),
                value.TextMap.Entry("entry2", Some(Value(Sum.Text("some text2")))),
              )
            )
          )
        ),
        DynamicValue.TextMap(
          Seq(
            "entry1" -> DynamicValue.Text("some text1"),
            "entry2" -> DynamicValue.Text("some text2"),
          )
        ),
        schemaProcessor.textMap(schemaProcessor.text),
      ),
    )
