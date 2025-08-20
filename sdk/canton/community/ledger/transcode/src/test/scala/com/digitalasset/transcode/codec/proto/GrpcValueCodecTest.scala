// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.proto

import com.daml.ledger.api.v2.value as v2Value
import com.daml.ledger.api.v2.value.Value.Sum as v2Sum
import com.digitalasset.canton.BaseTest
import com.digitalasset.daml.lf.archive.{DarParser, Decode}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.PackageSignature
import com.digitalasset.daml.lf.language.Util
import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.daml_lf.{SchemaEntity, SchemaProcessor}
import org.scalatest.wordspec.AnyWordSpecLike

import java.io.FileInputStream
import java.util.zip.ZipInputStream

class GrpcValueCodecTest extends BaseTest with AnyWordSpecLike {

  private val packages: Map[PackageId, PackageSignature] = DarParser
    .readArchive("dar", new ZipInputStream(new FileInputStream(CantonExamplesPath)))
    .value
    .all
    .map(a => Decode.decodeArchive(a).value)
    .map { case (id, pkg) => id -> Util.toSignature(pkg) }
    .toMap

  private val trailingNone: SchemaEntity.Template[Codec[v2Value.Value]] = SchemaProcessor
    .process(packages)(GrpcValueCodec)(identity)
    .value
    .collectFirst {
      case x: SchemaEntity.Template[_] if x.id.qualifiedName.name.dottedName == "TrailingNone" => x
    }
    .value

  "GrpcValueCodec record encoding" should {

    "keep populated trailing optional fields" in {

      val expected = v2Value.Value(
        v2Sum.Record(
          v2Value.Record(
            None,
            Seq(
              v2Value.RecordField("p", Some(v2Value.Value(v2Sum.Party("alice")))),
              v2Value.RecordField(
                "i",
                Some(
                  v2Value.Value(
                    v2Sum.Optional(v2Value.Optional(Some(v2Value.Value(v2Sum.Int64(123L)))))
                  )
                ),
              ),
            ),
          )
        )
      )
      val dyn = trailingNone.payload.toDynamicValue(expected)
      val actual = trailingNone.payload.fromDynamicValue(dyn)

      actual shouldBe expected
    }

    "drop unpopulated trailing optional fields" in {
      val expected = v2Value.Value(
        v2Sum.Record(
          v2Value.Record(
            None,
            Seq(
              v2Value.RecordField("p", Some(v2Value.Value(v2Sum.Party("alice"))))
            ),
          )
        )
      )
      val dyn = trailingNone.payload.toDynamicValue(expected)
      val actual = trailingNone.payload.fromDynamicValue(dyn)

      actual shouldBe expected
    }

    "drop empty trailing optional fields" in {
      val input = v2Value.Value(
        v2Sum.Record(
          v2Value.Record(
            None,
            Seq(
              v2Value.RecordField("p", Some(v2Value.Value(v2Sum.Party("alice")))),
              v2Value.RecordField("i", Some(v2Value.Value(v2Sum.Optional(v2Value.Optional(None))))),
            ),
          )
        )
      )
      val expected = v2Value.Value(
        v2Sum.Record(
          v2Value.Record(
            None,
            Seq(
              v2Value.RecordField("p", Some(v2Value.Value(v2Sum.Party("alice"))))
            ),
          )
        )
      )
      val dyn = trailingNone.payload.toDynamicValue(input)
      val actual = trailingNone.payload.fromDynamicValue(dyn)

      actual shouldBe expected
    }

  }

}
