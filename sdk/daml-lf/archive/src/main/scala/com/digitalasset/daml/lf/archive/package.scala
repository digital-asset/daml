// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.daml.scalautil.Statement.discard

import scala.util.Using
import scala.util.Using.Releasable
import scala.util.control.NonFatal

package object archive {

  @throws[Error]
  private[archive] def assertRight[X](e: Either[Error, X]): X =
    e match {
      case Right(value) => value
      case Left(error) => throw error
    }

  // like normal Using, but catches error when trying to open the resource
  private[archive] def using[R, X](where: => String, open: () => R)(f: R => Either[Error, X])(
      implicit releasable: Releasable[R]
  ) =
    attempt(where)(open()).flatMap(Using.resource(_)(f))

  private[archive] def attempt[X](where: => String)(x: => X): Either[Error, X] =
    try Right(x)
    catch {
      case error: java.io.IOException => Left(Error.IO(where, error))
      case error: Error => Left(error)
      case NonFatal(err) =>
        Left(
          Error.Internal(where, s"Unexpected ${err.getClass.getSimpleName} Exception", Some(err))
        )
    }

  // This constant is introduced and used
  // to make serialization of nested data
  // possible otherwise complex models failed to deserialize.
  private val EXTENDED_PROTOBUF_RECURSION_LIMIT: Int = 1000

  val ArchiveParser: GenReader[DamlLf.Archive] =
    GenReader(getClass.getCanonicalName + ".ArchiveParser", DamlLf.Archive.parseFrom)
  val ArchiveReader: GenReader[ArchivePayload] =
    ArchiveParser.andThen(Reader.readArchive(_, schemaMode = false))
  val ArchiveDecoder: GenReader[(PackageId, Ast.Package)] =
    ArchiveReader.andThen(Decode.decodeArchivePayload)
  val ArchiveSchemaReader: GenReader[ArchivePayload] =
    ArchiveParser.andThen(Reader.readArchive(_, schemaMode = true))
  val ArchiveSchemaDecoder: GenReader[(PackageId, Ast.PackageSignature)] =
    ArchiveSchemaReader.andThen(Decode.decodeArchivePayloadSchema)

  val ArchivePayloadParser: GenReader[DamlLf.ArchivePayload] =
    GenReader(
      getClass.getCanonicalName + ".ArchivePayloadParser",
      DamlLf.ArchivePayload.parseFrom,
    )
  val lf1PackageParser: GenReader[DamlLf1.Package] =
    GenReader(
      getClass.getCanonicalName + ".Lf1PackageParser",
      { cos =>
        discard(cos.setRecursionLimit(EXTENDED_PROTOBUF_RECURSION_LIMIT))
        DamlLf1.Package.parseFrom(cos)
      },
    )

  def lf2PackageParser(minor: LanguageVersion.Minor): GenReader[DamlLf2.Package] =
    GenReader(
      getClass.getCanonicalName + ".Lf2PackageParser",
      { cos =>
        val langVersion = LanguageVersion(LanguageVersion.Major.V2, minor)
        if (!LanguageVersion.featureFlatArchive.enabledIn(langVersion))
          discard(cos.setRecursionLimit(EXTENDED_PROTOBUF_RECURSION_LIMIT))
        DamlLf2.Package.parseFrom(cos)
      },
    )

  private def ModuleParser(ver: LanguageVersion): GenReader[DamlLf2.Package] =
    ver.major match {
      case LanguageVersion.Major.V2 =>
        GenReader(
          getClass.getCanonicalName + ".ModuleParser",
          { cos =>
            if (!LanguageVersion.featureFlatArchive.enabledIn(ver))
              discard(cos.setRecursionLimit(EXTENDED_PROTOBUF_RECURSION_LIMIT))
            DamlLf2.Package.parseFrom(cos)
          },
        )
      case LanguageVersion.Major.V1 =>
        GenReader.fail(Error.Parsing(s"LF version $ver unsupported"))
    }

  private[lf] def moduleDecoder(ver: LanguageVersion, pkgId: PackageId): GenReader[Ast.Module] =
    ModuleParser(ver).andThen(new DecodeV2(ver.minor).decodeSingleModulePackage(pkgId, _))

  val DarParser: GenDarReader[DamlLf.Archive] = GenDarReader(ArchiveParser)
  val DarReader: GenDarReader[ArchivePayload] = GenDarReader(ArchiveReader)
  val DarDecoder: GenDarReader[(PackageId, Ast.Package)] = GenDarReader(ArchiveDecoder)
  val DarSchemaReader: GenDarReader[ArchivePayload] = GenDarReader(ArchiveSchemaReader)
  val DarSchemaDecoder: GenDarReader[(PackageId, Ast.PackageSignature)] = GenDarReader(
    ArchiveSchemaDecoder
  )

  val UniversalArchiveReader: GenUniversalArchiveReader[ArchivePayload] =
    new GenUniversalArchiveReader(ArchiveReader)
  val UniversalArchiveDecoder: GenUniversalArchiveReader[(PackageId, Ast.Package)] =
    new GenUniversalArchiveReader(ArchiveDecoder)

}
