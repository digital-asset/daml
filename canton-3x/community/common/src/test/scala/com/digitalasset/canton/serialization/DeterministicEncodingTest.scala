// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.serialization

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.UByte
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class DeterministicEncodingTest extends AnyWordSpec with BaseTest {

  "DeterministicEncoding" when {
    val rest = ByteString.copyFromUtf8("rest")

    "working on Ints" should {
      val goodInputs =
        Table(
          ("int", "serialization"),
          (1, Array[Byte](0, 0, 0, 1)),
          (-1, Array[Byte](-1, -1, -1, -1)),
          (0x12345678, Array[Byte](0x12, 0x34, 0x56, 0x78)),
        )

      val badInputs = Table[Array[Byte]](
        "serialization",
        Array[Byte](),
        Array[Byte](0, 0, 0),
        Array[Byte](-1),
      )

      "produce the correct serialization" in {
        forAll(goodInputs) { (i, bytes) =>
          assert(DeterministicEncoding.encodeInt(i) === ByteString.copyFrom(bytes))
        }
      }
      "produce the correct deserialization" in {
        forAll(goodInputs) { (i, bytes) =>
          assert(
            DeterministicEncoding.decodeInt(ByteString.copyFrom(bytes)) === Right(
              (i, ByteString.EMPTY)
            )
          )
        }
      }

      "produce the correct deserialization and hand back the remaining bytes" in {
        forAll(goodInputs) { (i, bytes) =>
          assert(
            DeterministicEncoding.decodeInt(ByteString.copyFrom(bytes).concat(rest)) == Right(
              (i, rest)
            )
          )
        }
      }

      "fail if too few bytes are passed in" in {
        forAll(badInputs) { bytes =>
          assert(DeterministicEncoding.decodeInt(ByteString.copyFrom(bytes)).isLeft)
        }
      }
    }

    "working on Strings" should {
      val goodInputs =
        Table(
          ("string", "serialization"),
          ("", Array[Byte](0, 0, 0, 0)),
          (" @", Array[Byte](0, 0, 0, 2, 32, 64)),
        )

      val badInputs = Table[Array[Byte]](
        "serialization",
        Array[Byte](),
        Array[Byte](0, 0, 0, 2, 32),
        Array[Byte](0, 0, 2),
      )

      "produce the correct serialization" in {
        forAll(goodInputs) { (s, bytes) =>
          assert(DeterministicEncoding.encodeString(s) === ByteString.copyFrom(bytes))
        }
      }

      "produce the correct deserialization" in {
        forAll(goodInputs) { (s, bytes) =>
          assert(
            DeterministicEncoding.decodeString(ByteString.copyFrom(bytes)) === Right(
              (s, ByteString.EMPTY)
            )
          )
        }
      }

      "produce the correct deserialization and hand back the remaining bytes" in {
        forAll(goodInputs) { (s, bytes) =>
          assert(
            DeterministicEncoding.decodeString(ByteString.copyFrom(bytes).concat(rest)) == Right(
              (s, rest)
            )
          )
        }
      }

      "fail if invalid serializations are passed in" in {
        forAll(badInputs) { bytes =>
          assert(DeterministicEncoding.decodeString(ByteString.copyFrom(bytes)).isLeft)
        }
      }
    }

    "working on bytes" should {
      val (b1, b2, b3) =
        (Array[Byte](0, 1, 2, 3, 4, 5, 6), Array[Byte](), Array[Byte](6, 5, 4, 3, 2, 1))
      "yield the same inputs after a full cycle of one" in {
        val ec1 = DeterministicEncoding.encodeBytes(ByteString.copyFrom(b1))
        val tmp = DeterministicEncoding.decodeBytes(ec1)
        assert(tmp.isRight)
        tmp.foreach {
          case (o1, restB) => {
            assertResult(b1)(o1.toByteArray)
            assertResult(0)(restB.size)
          }
        }
      }
      "yield the same input after a full cycle of three" in {
        val ec2 = DeterministicEncoding
          .encodeBytes(ByteString.copyFrom(b1))
          .concat(DeterministicEncoding.encodeBytes(ByteString.copyFrom(b2)))
          .concat(DeterministicEncoding.encodeBytes(ByteString.copyFrom(b3)))
        val tmp = for {
          dc1AndR <- DeterministicEncoding.decodeBytes(ec2)
          dc2AndR <- DeterministicEncoding.decodeBytes(dc1AndR._2)
          dc3AndR <- DeterministicEncoding.decodeBytes(dc2AndR._2)
        } yield (dc1AndR._1, dc2AndR._1, dc3AndR._1, dc3AndR._2)
        assert(tmp.isRight)
        tmp.foreach {
          case (o1, o2, o3, rest) => {
            assertResult(b1)(o1.toByteArray)
            assertResult(b2)(o2.toByteArray)
            assertResult(b3)(o3.toByteArray)
            assertResult(0)(rest.size)
          }
        }
      }

      "fail on invalid length" in {
        val testLengthEncoding = Array[Byte](-127, 0, 0, 0)
        val tmp = DeterministicEncoding.decodeInt(ByteString.copyFrom(testLengthEncoding))
        assert(tmp.isRight)
        tmp.foreach(xx => assert(xx._1 < 0))
        val negativeLength = Array[Byte](-127, 0, 0, 0, 1, 2, 3, 4, 5, 6)
        assert(DeterministicEncoding.decodeBytes(ByteString.copyFrom(negativeLength)).isLeft)
        val lengthExceeded = Array[Byte](0, 0, 0, 127, 1, 2, 3, 4, 5, 6)
        assert(DeterministicEncoding.decodeBytes(ByteString.copyFrom(lengthExceeded)).isLeft)
      }
    }
    "working on sequences" should {
      "obtain original sequence" in {
        Seq(Seq(1), Seq(), Seq(1, 2, 3, 4, 5)).foreach(tst => {
          val res = DeterministicEncoding.decodeSeqWith(
            DeterministicEncoding.encodeSeqWith(tst)(DeterministicEncoding.encodeInt)
          )(DeterministicEncoding.decodeInt)
          res.value._1 shouldBe tst
          res.foreach { case (seq, rest) =>
            seq shouldBe tst
            assertResult(0)(rest.size)
          }
        })
      }
    }

    "working with unsigned var-ints" should {
      def arrayToByteString(array: Array[Int]): ByteString =
        UByte.fromArrayToByteString(array.map(UByte.tryFromUnsignedInt))

      val goodInputs =
        Table(
          ("long", "serialization"),
          (1L, Array[Int](1)),
          (127L, Array[Int](127)),
          (128L, Array[Int](128, 1)),
          (255L, Array[Int](255, 1)),
          (300L, Array[Int](172, 2)),
          (16384L, Array[Int](128, 128, 1)),
        )

      val badInputs = Table[Array[Int]](
        "serialization",
        // Input longer than 9 bytes
        Array[Int](128, 128, 128, 128, 128, 128, 128, 128, 128, 128),
      )

      "produce the correct serialization" in {
        forAll(goodInputs) { (i, bytes) =>
          assert(DeterministicEncoding.encodeUVarInt(i) === arrayToByteString(bytes))
        }
      }

      "produce the correct deserialization" in {
        forAll(goodInputs) { (i, bytes) =>
          assert(
            DeterministicEncoding.decodeUVarInt(arrayToByteString(bytes)) === Right(
              (i, ByteString.EMPTY)
            )
          )
        }
      }

      "produce the correct deserialization and hand back the remaining bytes" in {
        forAll(goodInputs) { (i, bytes) =>
          assert(
            DeterministicEncoding.decodeUVarInt(arrayToByteString(bytes).concat(rest)) == Right(
              (i, rest)
            )
          )
        }
      }

      "fail deserialization on bad input" in {
        forAll(badInputs) { bytes =>
          assert(DeterministicEncoding.decodeUVarInt(arrayToByteString(bytes)).isLeft)
        }
      }
    }
  }
}
