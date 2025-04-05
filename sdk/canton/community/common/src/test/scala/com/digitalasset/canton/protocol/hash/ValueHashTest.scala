// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.daml.lf.data
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, SortedLookupList, Time}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant

class ValueHashTest extends BaseTest with AnyWordSpecLike with HashUtilsTest {
  "ValueBuilder" should {
    def withValueBuilder(f: (LfValueBuilder, HashTracer.StringHashTracer) => Assertion) = {
      val hashTracer = HashTracer.StringHashTracer()
      val builder = LfValueHashBuilder.valueBuilderForV1Node(hashTracer)
      f(builder, hashTracer)
    }

    def assertEncode(value: Value, expectedHash: String, expectedDebugEncoding: String) =
      withValueBuilder { case (builder, hashTracer) =>
        val hash = builder.addTypedValue(value).finish()
        hash.toHexString shouldBe Hash.fromHexStringRaw(expectedHash).value.toHexString
        hashTracer.result shouldBe expectedDebugEncoding
        assertStringTracer(hashTracer, hash)
      }

    "encode unit value" in {
      assertEncode(
        Value.ValueUnit,
        "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
        """'00' # Unit Type Tag
          |""".stripMargin,
      )
    }

    "encode true value" in {
      assertEncode(
        Value.ValueBool(true),
        "9dcf97a184f32623d11a73124ceb99a5709b083721e878a16d78f596718ba7b2",
        """'01' # Bool Type Tag
          |'01' # true (bool)
          |""".stripMargin,
      )
    }

    "encode false value" in {
      assertEncode(
        Value.ValueBool(false),
        "47dc540c94ceb704a23875c11273e16bb0b8a87aed84de911f2133568115f254",
        """'01' # Bool Type Tag
          |'00' # false (bool)
          |""".stripMargin,
      )
    }

    "encode text value" in {
      assertEncode(
        Value.ValueText("hello world!"),
        "a24821b4741b3616920a37dfccf3b1e271184c82fc89377ab22e5d96d9330e5c",
        """'07' # Text Type Tag
          |'0000000c' # 12 (int)
          |'68656c6c6f20776f726c6421' # hello world! (string)
          |""".stripMargin,
      )
    }

    "encode numeric value" in {
      // Numerics are encoded from their string representation
      assertEncode(
        Value.ValueNumeric(data.Numeric.assertFromString("125.1002")),
        "fd23ea3b05b8b0e1d15902cccf7c6c0e6e292f0ae3e96513c8d8cec9d4f9bda9",
        """'03' # Numeric Type Tag
          |'00000008' # 8 (int)
          |'3132352e31303032' # 125.1002 (numeric)
          |""".stripMargin,
      )
    }

    "encode contract id value" in {
      assertEncode(
        Value.ValueContractId(
          Value.ContractId.V1
            .assertFromString("0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b")
        ),
        "6a3e241cebc1dc5e7574be0cf122edc9cb65ffc3fa59fb069c5ab3bbb4598414",
        """'08' # ContractId Type Tag
          |'00000021' # 33 (int)
          |'0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b' # 0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b (contractId)
          |""".stripMargin,
      )
    }

    "encode enum value" in {
      assertEncode(
        Value.ValueEnum(Some(defRef("module", "name")), Ref.Name.assertFromString("ENUM")),
        "9c8f627c22d6871e111e1f5d1980a6a24bcc6311bbc73fd3932a4d79fa9c09f2",
        """'0e' # Enum Type Tag
          |'01' # Some
          |'00000007' # 7 (int)
          |'7061636b616765' # package (string)
          |'00000001' # 1 (int)
          |'00000006' # 6 (int)
          |'6d6f64756c65' # module (string)
          |'00000001' # 1 (int)
          |'00000004' # 4 (int)
          |'6e616d65' # name (string)
          |'00000004' # 4 (int)
          |'454e554d' # ENUM (string)
          |""".stripMargin,
      )
    }

    "encode int64 value" in {
      assertEncode(
        Value.ValueInt64(10L),
        "a74730eb88baf5118934d3675ccf50eac1e4e873000ecd75b5917c78ee30ef26",
        """'02' # Int64 Type Tag
          |'000000000000000a' # 10 (long)
          |""".stripMargin,
      )
    }

    "encode variant value" in {
      assertEncode(
        Value.ValueVariant(
          Some(defRef("module", "name")),
          Ref.Name.assertFromString("ENUM"),
          Value.ValueTrue,
        ),
        "bceee77c48d80db35f237c3877c6711db19a8c135d2d0323c21f01afc2eb7612",
        """'0d' # Variant Type Tag
          |'01' # Some
          |'00000007' # 7 (int)
          |'7061636b616765' # package (string)
          |'00000001' # 1 (int)
          |'00000006' # 6 (int)
          |'6d6f64756c65' # module (string)
          |'00000001' # 1 (int)
          |'00000004' # 4 (int)
          |'6e616d65' # name (string)
          |'00000004' # 4 (int)
          |'454e554d' # ENUM (string)
          |'01' # Bool Type Tag
          |'01' # true (bool)
          |""".stripMargin,
      )
    }

    "encode list value" in {
      assertEncode(
        Value.ValueList(
          FrontStack.from(
            List(
              Value.ValueText("five"),
              Value.ValueInt64(5L),
              Value.ValueTrue,
            )
          )
        ),
        "6049408351d7be2c9d6ae7a1beecf08676fa72955c376c89571a1c5f04f272bd",
        """'0a' # List Type Tag
          |'00000003' # 3 (int)
          |'07' # Text Type Tag
          |'00000004' # 4 (int)
          |'66697665' # five (string)
          |'02' # Int64 Type Tag
          |'0000000000000005' # 5 (long)
          |'01' # Bool Type Tag
          |'01' # true (bool)
          |""".stripMargin,
      )
    }

    "encode text map value" in {
      assertEncode(
        Value.ValueTextMap(
          SortedLookupList(
            Map(
              "foo" -> Value.ValueNumeric(data.Numeric.assertFromString("31380.0")),
              "bar" -> Value.ValueText("1284"),
            )
          )
        ),
        "b318100aac8cde766598a0a5bc05451feaec7c90400f75de93100d0c87ec24cf",
        """'0b' # TextMap Type Tag
          |'00000002' # 2 (int)
          |'00000003' # 3 (int)
          |'626172' # bar (string)
          |'07' # Text Type Tag
          |'00000004' # 4 (int)
          |'31323834' # 1284 (string)
          |'00000003' # 3 (int)
          |'666f6f' # foo (string)
          |'03' # Numeric Type Tag
          |'00000007' # 7 (int)
          |'33313338302e30' # 31380.0 (numeric)
          |""".stripMargin,
      )
    }

    "encode gen map value" in {
      assertEncode(
        Value.ValueGenMap(
          ImmArray(
            (Value.ValueInt64(5L), Value.ValueText("five")),
            (Value.ValueInt64(10L), Value.ValueText("ten")),
          )
        ),
        "523c5247ed42efa40dd6b0165a01c1b14dcede7b39a1f33a3590fc7defc4a610",
        """'0f' # GenMap Type Tag
          |'00000002' # 2 (int)
          |'02' # Int64 Type Tag
          |'0000000000000005' # 5 (long)
          |'07' # Text Type Tag
          |'00000004' # 4 (int)
          |'66697665' # five (string)
          |'02' # Int64 Type Tag
          |'000000000000000a' # 10 (long)
          |'07' # Text Type Tag
          |'00000003' # 3 (int)
          |'74656e' # ten (string)
          |""".stripMargin,
      )
    }

    "encode optional empty value" in {
      assertEncode(
        Value.ValueOptional(None),
        "a2c4aed1cf757cd9a509734a267ffc7b1166b55f4c8f9c3e3550c56e743328fc",
        """'09' # Optional Type Tag
          |'00' # None
          |""".stripMargin,
      )
    }

    "encode optional defined value" in {
      assertEncode(
        Value.ValueOptional(Some(Value.ValueText("hello"))),
        "dfba7295cf094b6b3ffdf3e44411793264882b38c810f9dff63148890a466171",
        """'09' # Optional Type Tag
          |'01' # Some
          |'07' # Text Type Tag
          |'00000005' # 5 (int)
          |'68656c6c6f' # hello (string)
          |""".stripMargin,
      )
    }

    "encode timestamp value" in {
      assertEncode(
        // Thursday, 24 October 2024 16:43:46
        Value.ValueTimestamp(
          Time.Timestamp.assertFromInstant(Instant.ofEpochMilli(1729788226000L))
        ),
        "18efa9ad9adfa83fc3e84cdbe9c4b9f0e7305376e858e5bd909117209111c9d5",
        """'04' # Timestamp Type Tag
          |'0006253bb4bf5480' # 1729788226000000 (long)
          |""".stripMargin,
      )
    }

    "encode date value" in {
      assertEncode(
        // Thursday, 24 October 2024
        Value.ValueDate(Time.Date.assertFromDaysSinceEpoch(20020)),
        "6892b74f329df4e0fd850c0e4d35384d3a4aef0a2f0f32070172c1f121ff632f",
        """'05' # Date Type Tag
          |'00004e34' # 20020 (int)
          |""".stripMargin,
      )
    }

    "encode record value" in {
      assertEncode(
        Value.ValueRecord(
          Some(defRef("module", "name")),
          ImmArray(
            (
              Some(Ref.Name.assertFromString("field1")),
              Value.ValueTrue,
            ),
            (
              Some(Ref.Name.assertFromString("field2")),
              Value.ValueText("hello"),
            ),
          ),
        ),
        "ae594f7684700c299af9bc0b83758f2d71836ae6d9ea628d310f0305698df6f2",
        """'0c' # Record Type Tag
          |'01' # Some
          |'00000007' # 7 (int)
          |'7061636b616765' # package (string)
          |'00000001' # 1 (int)
          |'00000006' # 6 (int)
          |'6d6f64756c65' # module (string)
          |'00000001' # 1 (int)
          |'00000004' # 4 (int)
          |'6e616d65' # name (string)
          |'00000002' # 2 (int)
          |'01' # Some
          |'00000006' # 6 (int)
          |'6669656c6431' # field1 (string)
          |'01' # Bool Type Tag
          |'01' # true (bool)
          |'01' # Some
          |'00000006' # 6 (int)
          |'6669656c6432' # field2 (string)
          |'07' # Text Type Tag
          |'00000005' # 5 (int)
          |'68656c6c6f' # hello (string)
          |""".stripMargin,
      )
    }
  }
}
