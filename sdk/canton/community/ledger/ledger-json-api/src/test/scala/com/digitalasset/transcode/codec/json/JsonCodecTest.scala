// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.json

import com.digitalasset.canton.BaseTest
import com.digitalasset.daml.lf.archive.{DarParser, Decode}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.PackageSignature
import com.digitalasset.daml.lf.language.Util
import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.daml_lf.LfSchemaProcessor
import com.digitalasset.transcode.schema.{IdentifierFilter, Template}
import org.scalatest.LoneElement
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.io.FileInputStream
import java.util.zip.ZipInputStream

class JsonCodecTest extends BaseTest with AnyWordSpecLike with Matchers with LoneElement {

  private val packages: Map[PackageId, PackageSignature] = DarParser
    .readArchive("dar", new ZipInputStream(new FileInputStream(CantonExamplesPath)))
    .value
    .all
    .map(a => Decode.decodeArchive(a).value)
    .map { case (id, pkg) => id -> Util.toSignature(pkg) }
    .toMap

  private val trailingNone: Template[Codec[ujson.Value]] = LfSchemaProcessor
    .process(packages, IdentifierFilter.AcceptAll)(
      new JsonCodec(removeTrailingNonesInRecords = true)
    )
    .value
    .entities
    .collectFirst { case x if x.templateId.entityName == "TrailingNone" => x }
    .value

  "JsonCodec record encoding" should {

    "keep populated trailing optional fields" in {

      val expected = ujson.Obj("p" -> ujson.Str("alice"), "i" -> ujson.Str("123"))
      val dyn = trailingNone.payload.toDynamicValue(expected)
      val actual = trailingNone.payload.fromDynamicValue(dyn)

      actual shouldBe expected
    }

    "work with fields in any order" in {

      val expected = ujson.Obj("i" -> ujson.Str("123"), "p" -> ujson.Str("alice"))
      val dyn = trailingNone.payload.toDynamicValue(expected)
      val actual = trailingNone.payload.fromDynamicValue(dyn)

      actual shouldBe expected
    }

    "drop unpopulated trailing optional fields" in {

      val expected = ujson.Obj("p" -> ujson.Str("alice"))
      val dyn = trailingNone.payload.toDynamicValue(expected)
      val actual = trailingNone.payload.fromDynamicValue(dyn)

      actual shouldBe expected
    }

    "drop empty trailing optional fields" in {

      val input = ujson.Obj("p" -> ujson.Str("alice"), "i" -> ujson.Null)
      val expected = ujson.Obj("p" -> ujson.Str("alice"))
      val dyn = trailingNone.payload.toDynamicValue(input)
      val actual = trailingNone.payload.fromDynamicValue(dyn)

      actual shouldBe expected
    }

  }

}
