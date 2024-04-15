// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.typesig

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.daml.lf.typesig.reader.{DamlLfArchiveReader, Errors, SignatureReader}
import com.daml.lf.{archive, typesig}
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, EitherValues, OptionValues}

import java.nio.file.Paths
import EncoderDecoder._
import com.daml.lf.typesig.PackageSignature.TypeDecl.Template

object EDS {
  case class FullFoo(a: Int, b: String, c: Boolean)

  case class SemiFoo(a: Int, b: String, c: Boolean)
  implicit val fooDecoder: Decoder[SemiFoo] = deriveDecoder[SemiFoo]
  implicit val fooEncoder: Encoder[SemiFoo] = deriveEncoder[SemiFoo]

}

class EncoderDecoderSpec extends AnyWordSpec with Matchers with EitherValues with OptionValues {

  import EDS._

  // Build by hand
  private val darFile = Paths.get(
    "/Users/simonmaxen/dev/digital-asset/daml3x/sdk/bazel-bin/language-support/java/codegen/ledger-tests-model.dar"
  )

  "full generic encode/decode" in {
    import io.circe.generic.auto._
    val expected = FullFoo(1, "two", c = false)
    val json = expected.asJson.noSpaces
    val actual = decode[FullFoo](json).value
    actual shouldBe expected
  }

  "semi generic encode/decode" in {
    val expected = SemiFoo(1, "two", c = false)
    val json = expected.asJson.noSpaces
    val actual = decode[SemiFoo](json).value
    actual shouldBe expected
  }

  private def testEC[T: Encoder: Decoder](expected: T): Assertion = {
    val json = expected.asJson.spaces2
    println(json)
    val actual = decode[T](json).value
    actual shouldBe expected
  }

  "cycle foo encode/decode" in {
    testEC(SemiFoo(1, "two", c = false))
  }

  private def assertReadPackageSignature(lf: DamlLf.Archive): typesig.PackageSignature = {
    val (err, sig) = SignatureReader.readPackageSignature(lf)
    if (!err.equals(Errors.zeroErrors)) {
      fail(err.toString)
    }
    sig
  }

  "read dar" in {
    val dar: Dar[DamlLf.Archive] = archive.DarParser.readArchiveFromFile(darFile.toFile).value
    val (pkgId, pkg): (PackageId, Ast.Package) =
      DamlLfArchiveReader.readPackage(dar.main).toOption.value
    val sig = assertReadPackageSignature(dar.main)
    sig.packageId shouldBe pkgId
    sig.metadata.name shouldBe pkg.metadata.name
    println("model sig")
    testEC(sig)
  }

  "empty sig encode/decode" in {

    val expected = PackageSignature(
      packageId = PackageId.assertFromString("package-id"),
      metadata = PackageMetadata(
        Ref.PackageName.assertFromString("package-name"),
        Ref.PackageVersion.assertFromString("1.0.0"),
      ),
      typeDecls = Map.empty[Ref.QualifiedName, PackageSignature.TypeDecl],
      interfaces = Map.empty[Ref.QualifiedName, DefInterface.FWT],
    )

    testEC(expected)

  }

  "fragments" in {
    val dar: Dar[DamlLf.Archive] = archive.DarParser.readArchiveFromFile(darFile.toFile).value
    val sig = assertReadPackageSignature(dar.main)

    val expected = sig.typeDecls.values.collect({ case t: Template => t }).head.template
    testEC(expected)
  }

}
