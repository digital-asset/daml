// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.daml.lf.archive.DamlLf.Archive
import com.digitalasset.daml.lf.archive.testing.Encode
import com.digitalasset.daml.lf.archive.{Dar as LfDar, DarWriter}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageName
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.google.protobuf.ByteString
import org.scalatest.TryValues.convertTryToSuccessOrFailure

import scala.util.Using

object PackageTestUtils {

  def astPackageFromLfDef(defn: ParserParameters[?] => Ast.Package)(
      lfVersion: LanguageVersion = LanguageVersion.v2_1,
      packageId: Ref.PackageId = Ref.PackageId.assertFromString("-self-"),
  ): Ast.Package =
    defn(
      ParserParameters(
        defaultPackageId = packageId,
        languageVersion = lfVersion,
      )
    )

  def archiveFromLfDef(defn: ParserParameters[?] => Ast.Package)(
      lfVersion: LanguageVersion = LanguageVersion.v2_1,
      packageId: Ref.PackageId = Ref.PackageId.assertFromString("-self-"),
  ): Archive = {
    val pkg = astPackageFromLfDef(defn)(lfVersion, packageId)
    Encode.encodeArchive(packageId -> pkg, lfVersion)
  }

  def sampleAstPackage(
      packageName: PackageName,
      packageVersion: Ref.PackageVersion,
      discriminatorFields: Seq[String] = Seq.empty,
  )(
      lfVersion: LanguageVersion = LanguageVersion.v2_1,
      packageId: Ref.PackageId = Ref.PackageId.assertFromString("-self-"),
  ): Ast.Package =
    astPackageFromLfDef(implicit parseParameters =>
      p"""
        metadata ( '$packageName' : '${packageVersion.toString}' )
        module Mod {
          record @serializable T = { actor: Party ${if (discriminatorFields.isEmpty) ""
        else discriminatorFields.mkString(", ", ", ", "")}};

          template (this: T) = {
            precondition True;
            signatories Cons @Party [Mod:T {actor} this] (Nil @Party);
            observers Nil @Party;
          };
       }"""
    )(lfVersion, packageId)

  def sampleLfArchive(
      packageName: Ref.PackageName,
      packageVersion: Ref.PackageVersion,
      discriminatorFields: Seq[String] = Seq.empty,
      lfVersion: LanguageVersion = LanguageVersion.v2_1,
      packageId: Ref.PackageId = Ref.PackageId.assertFromString("-self-"),
  ): Archive = {
    val pkg = sampleAstPackage(
      packageName = packageName,
      packageVersion = packageVersion,
      discriminatorFields = discriminatorFields,
    )(
      lfVersion = lfVersion,
      packageId = packageId,
    )
    Encode.encodeArchive(packageId -> pkg, lfVersion)
  }

  implicit class ArchiveOps(archive: Archive) {
    def lfArchiveToByteString: ByteString = Using(ByteString.newOutput()) { os =>
      DarWriter.encode(
        BuildInfo.damlLibrariesVersion,
        LfDar(("archive.dalf", archive.toByteArray), List()),
        os,
      )
      os.toByteString
    }.success.value
  }
}
