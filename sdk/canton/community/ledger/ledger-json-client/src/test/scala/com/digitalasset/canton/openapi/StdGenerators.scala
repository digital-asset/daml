// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.openapi

import org.scalacheck.{Arbitrary, Gen}

import java.time.Instant

object StdGenerators {
  // Three is no reason to generate larger strings or sequences
  private val maxStringLength = 20
  private val maxSeqSize = 5
  val shortAlphaNumStr: Gen[String] = for {
    len <- Gen.choose(1, maxStringLength) // short strings: 1 to 10 characters
    chars <- Gen.listOfN(len, Gen.alphaNumChar)
  } yield chars.mkString
  implicit val arbShortAlphaNumStr: Arbitrary[String] = Arbitrary(shortAlphaNumStr)

  implicit val arbJson: Arbitrary[io.circe.Json] = Arbitrary {
    io.circe.Json.obj(("field1", io.circe.Json.fromString("value1")))
  }

  def smallSeqArbitrary[T](implicit arbT: Arbitrary[T]): Arbitrary[Seq[T]] =
    Arbitrary(Gen.choose(0, maxSeqSize).flatMap(n => Gen.listOfN(n, arbT.arbitrary)))

  implicit val arbTimestamp: Arbitrary[com.google.protobuf.timestamp.Timestamp] = Arbitrary {
    // get random time instant
    val randomInstantArb: Arbitrary[Instant] = implicitly[Arbitrary[Instant]]
    val instant = randomInstantArb.arbitrary.sample.getOrElse(Instant.now())
    com.google.protobuf.timestamp.Timestamp(instant.getEpochSecond, instant.getNano)
  }
  implicit val arbUfs: Arbitrary[scalapb.UnknownFieldSet] = Arbitrary {
    scalapb.UnknownFieldSet()
  }
  implicit val arbBs: Arbitrary[com.google.protobuf.ByteString] = Arbitrary {
    com.google.protobuf.ByteString.copyFromUtf8("kopytko")
  }
  // limit size of some sequences
  import magnolify.scalacheck.auto.*
  implicit val arbProtoAnySeq: Arbitrary[Seq[com.google.protobuf.any.Any]] =
    smallSeqArbitrary
}
