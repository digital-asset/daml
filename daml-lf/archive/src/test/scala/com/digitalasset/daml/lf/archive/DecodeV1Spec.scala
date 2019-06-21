// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml_lf.DamlLf1
import com.digitalasset.daml_lf.DamlLf1.PackageRef

import org.scalatest.{Inside, Matchers, WordSpec}

import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class DecodeV1Spec extends WordSpec with Matchers with Inside {

  "The keys of primTypeTable correspond to Protobuf DamlLf1.PrimType" in {

    (DecodeV1.primTypeTable.keySet + DamlLf1.PrimType.UNRECOGNIZED) shouldBe
      DamlLf1.PrimType.values().toSet

  }

  "The keys of builtinFunctionMap correspond to Protobuf DamlLf1.BuiltinFunction" in {

    (DecodeV1.builtinFunctionMap.keySet + DamlLf1.BuiltinFunction.UNRECOGNIZED) shouldBe
      DamlLf1.BuiltinFunction.values().toSet

  }

  "decodeModuleRef" should {
    lazy val ((pkgId, dalfProto), majorVersion) = {
      val dalfFile =
        Files.newInputStream(Paths.get(rlocation("daml-lf/archive/DarReaderTest-dev.dalf")))
      try Reader.readArchiveAndVersion(dalfFile)
      finally dalfFile.close()
    }

    lazy val Some(extId) = {
      val dalf1 = dalfProto.getDamlLf1
      val Some(iix) = dalf1.getModules(0).getValuesList.asScala collectFirst {
        case dv if dv.getNameWithType.getNameList.asScala.lastOption contains "reverseCopy" =>
          val pr = dv.getExpr.getVal.getModule.getPackageRef
          pr.getSumCase shouldBe PackageRef.SumCase.INTERNED_ID
          pr.getInternedId
      }
      dalf1.getInternedPackageIdsList.asScala.lift(iix.toInt)
    }

    "take a dalf with interned IDs" in {
      majorVersion should ===(LanguageMajorVersion.V1)

      // when the following line fails, change to !==("dev"),
      // use DarReaderTest.dalf instead in the val dalfFile above, and
      // delete the DarReaderTest-dev rule from archive's BUILD.bazel -SC
      dalfProto.getMinor should ===(DecodeV1.internedIdsVersion.toProtoIdentifier)

      extId should not be empty
      (extId: String) should !==(pkgId: String)
    }

    "decode resolving the interned package ID" in {
      val decoder = Decode.decoders(LanguageVersion(majorVersion, dalfProto.getMinor))
      inside(
        decoder.decoder
          .decodePackage(pkgId, decoder.extract(dalfProto))
          .lookupIdentifier(Ref.QualifiedName assertFromString "DarReaderTest:reverseCopy")) {
        case Right(
            Ast.DValue(_, _, Ast.ELocation(_, Ast.EVal(Ref.Identifier(resolvedExtId, _))), _)) =>
          (resolvedExtId: String) should ===(extId: String)
      }
    }
  }

}
