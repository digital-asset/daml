// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.SdkVersion
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.lf.archive.testing.Encode
import com.daml.lf.archive.{Dar as LfDar, DarWriter}
import com.daml.lf.data.Ref
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.testing.parser.ParserParameters
import com.google.protobuf.ByteString

import scala.util.Using

object PackagesTestUtils {

  def createLfArchive(defn: ParserParameters[?] => Ast.Package)(
      lfVersion: LanguageVersion = LanguageVersion.v1_16,
      packageId: Ref.PackageId = Ref.PackageId.assertFromString("-self-"),
  ): Archive = {
    implicit val parseParameters: ParserParameters[Nothing] = ParserParameters(
      defaultPackageId = packageId,
      languageVersion = lfVersion,
    )

    val pkg = defn(parseParameters)

    Encode.encodeArchive(packageId -> pkg, lfVersion)
  }

  def lfArchiveTemplate(
      packageName: Ref.PackageName,
      packageVersion: Ref.PackageVersion,
      discriminatorFields: String,
      lfVersion: LanguageVersion = LanguageVersion.v1_16,
      packageId: Ref.PackageId = Ref.PackageId.assertFromString("-self-"),
  ): Archive = createLfArchive { implicit parserParameters =>
    p"""
        metadata ( '$packageName' : '${packageVersion.toString}' )
        module Mod {
          record @serializable T = { actor: Party, $discriminatorFields };

          template (this: T) = {
            precondition True;
            signatories Cons @Party [Mod:T {actor} this] (Nil @Party);
            observers Nil @Party;
            agreement "Agreement";
          };
       }"""
  }(lfVersion, packageId)

  def encodeDarArchive(archive: Archive): ByteString =
    Using(ByteString.newOutput()) { os =>
      DarWriter.encode(
        SdkVersion.sdkVersion,
        LfDar(("archive.dalf", archive.toByteArray), List()),
        os,
      )
      os.toByteString
    }.get
}
