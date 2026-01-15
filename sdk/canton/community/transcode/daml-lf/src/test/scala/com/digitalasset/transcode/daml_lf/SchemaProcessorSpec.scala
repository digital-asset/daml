// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.daml_lf

import com.digitalasset.transcode.conformance.RoundtripId
import com.digitalasset.transcode.conformance.data.All
import com.digitalasset.transcode.schema.*
import zio.test.*

import scala.language.implicitConversions

object SchemaProcessorSpec extends SchemaProcessorSpecDefault:
  override def spec = suite("SchemaProcessorSpec")(
    suite("Roundtrip")(
      All.cases
        .map((name, descriptor, _) => (name, descriptor))
        .distinct
        .map((name, descriptor) =>
          test(name) {
            val arg = getSchema.choiceArgument(RoundtripId, ChoiceName(name))
            val Descriptor.Record.Ctor(_, _, List((_, payloadDescriptor))) = arg: @unchecked
            val resultDescriptor = getSchema.choiceResult(RoundtripId, ChoiceName(name))
            TestResult.allSuccesses(
              assert(payloadDescriptor)(equalTo(resultDescriptor)),
              assert(descriptor)(equalTo(payloadDescriptor)),
              assert(descriptor)(equalTo(resultDescriptor)),
            )
          }
        )
    ),
    test("pickling round trip") {
      val pickledSchema = Schema.serialize(getSchema)
      val unpickledSchema = Schema.deserialize(pickledSchema)
      assertTrue(unpickledSchema == getSchema)
    },
    suite("Other")(
      testSchema(
        "Other.InterfacesImpl",
        template(
          "Other.InterfacesImpl:InterfaceTypes",
          "cid1" -> Descriptor.contractId(
            Descriptor.constructor(
              "Other.Interfaces:MyInterface",
              Descriptor.record("name" -> Descriptor.text),
            )
          ),
          "cid2" -> Descriptor.contractId(
            Descriptor.constructor(
              "Other.Foo:AnyContract",
              Descriptor.record(),
            )
          ),
        ),
      ),
      testSchema(
        "Other.ChoiceAsDatatype",
        template(
          "Other.ChoiceAsDatatype:TheTemplate",
          "party" -> Descriptor.party,
          "choice" ->
            Descriptor.constructor(
              "Other.ChoiceAsDatatype:TheChoice",
              Descriptor.record(),
            ),
        ),
      ),
    ),
  )
