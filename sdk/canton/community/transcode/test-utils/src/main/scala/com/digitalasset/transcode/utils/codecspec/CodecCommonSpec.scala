// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.utils.codecspec

import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.conformance.RoundtripId
import com.digitalasset.transcode.conformance.data.All
import com.digitalasset.transcode.schema.*
import com.digitalasset.transcode.utils.propertygenerators.SchemaGenerator
import zio.test.*
import zio.test.Assertion.*
import zio.test.TestAspect.samples
import zio.{Chunk, durationInt}

import scala.language.reflectiveCalls
import scala.util.Try

trait CodecCommonSpec[P] extends ZIOSpecDefault:

  val schemaProcessor: CodecVisitor[P] | SchemaVisitor.WithResult[Dictionary[Codec[P]]]

  def testCodec[T](
      label: String,
      value: P,
      dynamicValue: DynamicValue,
      codec0: T,
  )(using conv: Conversion[T, Codec[P]]) = test(label) {
    val codec = conv(codec0)
    val decodedValue = codec.toDynamicValue(value)
    val encodedValue = codec.fromDynamicValue(dynamicValue)
    val roundtrip1 = codec.toDynamicValue(codec.fromDynamicValue(dynamicValue))
    val roundtrip2 = codec.fromDynamicValue(codec.toDynamicValue(value))
    assertTrue(
      DynamicValue.equals(decodedValue, dynamicValue)
        && encodedValue == value
        && DynamicValue.equals(roundtrip1, dynamicValue)
        && roundtrip2 == value
    )
  }

  lazy val autoTests = suite("auto tests")(
    suite("conformance tests")(
      All.cases.map { (name, d, expectedDv) =>
        val codec = getCodec(name, d)
        test(name)(verifyRoundtrip(codec, expectedDv))
      }*
    ),
    suite("upgrade forward incompatibility")(
      All.failing.map { (name, msg, d, dv) =>
        val codec = getCodec(name, d)
        test(name)(
          assert(Try(codec.fromDynamicValue(dv)))(isFailure(hasMessage(containsString(msg))))
        )
      }
    ),
    test(
      "roundtrip property: for each dynamic value encoding and decoding it provides the same dynamic value"
    )(
      check {
        for {
          schema0 <- SchemaGenerator.generateSchema
          schema = Schema.deserialize(Schema.serialize(schema0))
          codecs = DescriptorSchemaProcessor.process(schema, schemaProcessor).getOrElse(err)
          unit = DescriptorSchemaProcessor
            .process(schema, UnitVisitor)
            .getOrElse(err) // Verify unit finishes
          entities <- SchemaGenerator.dynamicEntityGen(schema)
        } yield (codecs, entities)
      } { (codecs, entities) =>
        TestResult.allSuccesses(
          codecs
            .zipWith(entities)(verifyRoundtrip)
            .entities
            .flatMap(t =>
              Seq(t.payload) ++ t.key.toSeq ++ t.choices.map(_.argument) ++ t.choices.map(_.result)
            )
        )
      }
    ) @@ samples(1000),
  )

  // Increase timeout to prevent warnings from failing the CI
  // The roundtrip property takes more than 60 seconds to run
  override def aspects: Chunk[TestAspectAtLeastR[Environment with TestEnvironment]] =
    Chunk(TestAspect.fibers, TestAspect.timeoutWarning(120.seconds))

  private def getCodec(name: String, d: Descriptor) =
    val choice = Choice(ChoiceName(name), true, Descriptor.record(), d)
    val template = Template(RoundtripId, Descriptor.record(), None, false, Seq.empty, Seq(choice))
    val schema = Dictionary(Seq(template))
    val codecs = DescriptorSchemaProcessor.assertProcess(schema, schemaProcessor)
    codecs.choiceResult(RoundtripId, ChoiceName(name))

  private def verifyRoundtrip(codec: Codec[P], dv1: DynamicValue) =
    val value1 = codec.fromDynamicValue(dv1)
    val dv2 = codec.toDynamicValue(value1)
    val value2 = codec.fromDynamicValue(dv2)
    val dv3 = codec.toDynamicValue(value2)
    assertTrue(
      DynamicValue.equals(dv1, dv2),
      DynamicValue.equals(dv2, dv3),
      DynamicValue.equals(dv1, dv3),
      value1 == value2,
    )

  given Conversion[schemaProcessor.Type, Codec[P]] = {
    case codec: CodecVisitor.Codec[P] @unchecked =>
      new Codec[P] {
        def fromDynamicValue(dv: DynamicValue): P = codec.fromDynamicValue(dv)(using Map.empty)
        def toDynamicValue(v: P): DynamicValue = codec.toDynamicValue(v)(using Map.empty)
      }
    case codec: Codec[P] @unchecked => codec
  }

  private def err = throw RuntimeException("Can't process schema")
  private def UnitVisitor = new SchemaVisitor.Unit {
    type Result = Unit
    def collect(entities: Seq[Template[Unit]]) = {}
  }
