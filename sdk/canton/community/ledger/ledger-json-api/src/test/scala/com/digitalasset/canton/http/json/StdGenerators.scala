// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.google.protobuf.timestamp.Timestamp
import org.scalacheck.{Arbitrary, Gen}

import java.time.Instant

object StdGenerators {
  // Three is no reason to generate larger strings or sequences
  private val maxStringLength = 20
  private val maxSeqSize = 5
  // We generate daml-lf compatible strings
  val shortDamlLfIdStr: Gen[String] = for {
    len <- Gen.choose(1, maxStringLength)
    firstChar <- Gen.alphaChar
    chars <- Gen.listOfN(len, Gen.alphaNumChar)
  } yield firstChar +: chars.mkString

  implicit val arbDamlLfStr: Arbitrary[String] = Arbitrary(shortDamlLfIdStr)

  implicit val arbJson: Arbitrary[io.circe.Json] = Arbitrary {
    io.circe.Json.obj(("field1", io.circe.Json.fromString("value1")))
  }

  def smallSeqArbitrary[T](implicit arbT: Arbitrary[T]): Arbitrary[Seq[T]] =
    Arbitrary(Gen.choose(0, maxSeqSize).flatMap(n => Gen.listOfN(n, arbT.arbitrary)))

  implicit val arbTimestamp: Arbitrary[Timestamp] =
    Arbitrary(Arbitrary.arbitrary[Instant].map(i => Timestamp(i.getEpochSecond, i.getNano)))

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
