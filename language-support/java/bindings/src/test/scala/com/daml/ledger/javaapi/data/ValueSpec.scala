// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import java.time.Instant
import java.util.{Optional => JOptional}
import java.util.concurrent.TimeUnit

import com.daml.ledger.javaapi.data.Generators._
import com.daml.ledger.api.v1.ValueOuterClass.Value.SumCase
import org.scalacheck.Gen
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class ValueSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TableDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "Value.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    valueGen
  ) { value =>
    Value.fromProto(value).toProto shouldEqual value
  }

  val conversions = Map[SumCase, (Value => JOptional[_], String)](
    SumCase.BOOL -> (((_: Value).asBool(), "asBool")),
    SumCase.CONTRACT_ID -> (((_: Value).asContractId(), "asContractId")),
    SumCase.DATE -> (((_: Value).asDate(), "asDate")),
    SumCase.NUMERIC -> (((_: Value).asNumeric(), "asNumeric")),
    SumCase.INT64 -> (((_: Value).asInt64(), "asInt64")),
    SumCase.LIST -> (((_: Value).asList(), "asList")),
    SumCase.PARTY -> (((_: Value).asParty(), "asParty")),
    SumCase.RECORD -> (((_: Value).asRecord(), "asRecord")),
    SumCase.TEXT -> (((_: Value).asText(), "asText")),
    SumCase.TIMESTAMP -> (((_: Value).asTimestamp(), "asTimestamp")),
    SumCase.UNIT -> (((_: Value).asUnit(), "asUnit")),
    SumCase.VARIANT -> (((_: Value).asVariant(), "asVariant")),
    SumCase.OPTIONAL -> (((_: Value).asOptional(), "asOptional")),
    SumCase.MAP -> (((_: Value).asTextMap(), "asTextMap")),
    SumCase.GEN_MAP -> (((_: Value).asGenMap(), "asGenMap")),
  )

  def assertConversions[T <: Value](sumCase: SumCase, expected: T): scala.Unit = {
    assertSuccessfulConversion(sumCase, expected)
    assertUnsuccessfulConversions(expected, sumCase)
  }

  def assertSuccessfulConversion[T <: Value](sumCase: SumCase, expected: T): scala.Unit = {
    val (conversion, name) = conversions(sumCase)
    s"Value.$name()" should s" work on ${value.getClass.getSimpleName}} instances" in {
      val converted = conversion(expected.asInstanceOf[Value])
      withClue(s"expected: ${expected.toString} converted: ${converted.toString}") {
        converted shouldEqual JOptional.of(expected)
      }
    }
  }

  def assertUnsuccessfulConversions(value: Value, excludedSumCase: SumCase): scala.Unit = {
    for ((conversion, name) <- conversions.view.filterKeys(_ != excludedSumCase).toMap.values) {
      s"Value.$name()" should s" should return Optional.empty() for ${value.getClass.getSimpleName} instances" in {
        conversion(value) shouldEqual JOptional.empty()
      }
    }
  }

  assertConversions(SumCase.BOOL, Value.fromProto(boolValueGen.sample.get))
  assertConversions(SumCase.CONTRACT_ID, Value.fromProto(contractIdValueGen.sample.get))
  assertConversions(SumCase.DATE, Value.fromProto(dateValueGen.sample.get))
  assertConversions(SumCase.NUMERIC, Value.fromProto(decimalValueGen.sample.get))
  assertConversions(SumCase.INT64, Value.fromProto(int64ValueGen.sample.get))
  assertConversions(SumCase.LIST, Value.fromProto(listValueGen.sample.get))
  assertConversions(SumCase.PARTY, Value.fromProto(partyValueGen.sample.get))
  assertConversions(SumCase.RECORD, Value.fromProto(recordValueGen.sample.get))
  assertConversions(SumCase.TEXT, Value.fromProto(textValueGen.sample.get))
  assertConversions(SumCase.TIMESTAMP, Value.fromProto(timestampValueGen.sample.get))
  assertConversions(SumCase.UNIT, Value.fromProto(unitValueGen.sample.get))
  assertConversions(SumCase.VARIANT, Value.fromProto(variantValueGen.sample.get))
  assertConversions(SumCase.OPTIONAL, Value.fromProto(optionalValueGen.sample.get))
  assertConversions(SumCase.MAP, Value.fromProto(textMapValueGen.sample.get))
  assertConversions(SumCase.GEN_MAP, Value.fromProto(genMapValueGen.sample.get))

  "Timestamp" should
    "be constructed from Instant" in forAll(Gen.posNum[Long]) { micros =>
      val expected = new Timestamp(micros)

      val instant =
        Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(micros), micros % 1000 * 1000)
      val timestampFromInstant = Timestamp.fromInstant(instant)
      expected shouldEqual timestampFromInstant
    }
  "Timestamp" should
    "be constructed from millis" in forAll(Gen.posNum[Long]) { millis =>
      val expected = new Timestamp(millis * 1000)

      val timestampFromMillis = Timestamp.fromMillis(millis)
      expected shouldEqual timestampFromMillis
    }

}
