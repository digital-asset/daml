package com.digitalasset.daml.lf.archive

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml_lf.DamlLf

object Encode extends Writer[(PackageId, Package)] {

  override protected def encodePayloadOfVersion(
      idAndPkg: (PackageId, Package),
      version: LanguageVersion
  ): DamlLf.ArchivePayload = {

    val (pkgId, pkg) = idAndPkg
    val LanguageVersion(major, minor) = version

    major match {
      case LanguageMajorVersion.V1 =>
        DamlLf.ArchivePayload
          .newBuilder()
          .setMinor(minor.toProtoIdentifier)
          .setDamlLf1(new EncodeV1(minor).encodePackage(pkgId, pkg))
          .build()
      case _ =>
        sys.error(s"$version not supported")
    }
  }

}
