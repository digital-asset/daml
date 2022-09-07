// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.daml_lf_dev.{DamlLf, DamlLf1}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard
import com.google.protobuf.CodedInputStream

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
  private val PROTOBUF_RECURSION_LIMIT: Int = 1000

  // just set the recursion limit
  private[this] val Base: GenReader[CodedInputStream] =
    new GenReader[CodedInputStream]({ cos =>
      discard(cos.setRecursionLimit(PROTOBUF_RECURSION_LIMIT))
      Right(cos)
    })

  val ArchiveParser: GenReader[DamlLf.Archive] =
    Base.andThen(cos =>
      attempt(getClass.getCanonicalName + ".ArchiveParser")(DamlLf.Archive.parseFrom(cos))
    )
  val ArchiveReader: GenReader[ArchivePayload] =
    ArchiveParser.andThen(Reader.readArchive)
  val ArchiveDecoder: GenReader[(PackageId, Ast.Package)] =
    ArchiveReader.andThen(Decode.decodeArchivePayload(_))

  val ArchivePayloadParser: GenReader[DamlLf.ArchivePayload] =
    Base.andThen(cos =>
      attempt(getClass.getCanonicalName + ".ArchivePayloadParser")(
        DamlLf.ArchivePayload.parseFrom(cos)
      )
    )
  def archivePayloadDecoder(
      hash: PackageId,
      onlySerializableDataDefs: Boolean = false,
  ): GenReader[(PackageId, Ast.Package)] =
    ArchivePayloadParser
      .andThen(Reader.readArchivePayload(hash, _))
      .andThen(Decode.decodeArchivePayload(_, onlySerializableDataDefs))

  private[lf] def moduleDecoder(ver: LanguageVersion, pkgId: PackageId): GenReader[Ast.Module] =
    Base
      .andThen(cos => attempt(NameOf.qualifiedNameOfCurrentFunc)(DamlLf1.Package.parseFrom(cos)))
      .andThen(new DecodeV1(ver.minor).xdecodeScenarioModule(pkgId, _))

  val DarParser: GenDarReader[DamlLf.Archive] = GenDarReader(ArchiveParser)
  val DarReader: GenDarReader[ArchivePayload] = GenDarReader(ArchiveReader)
  val DarDecoder: GenDarReader[(PackageId, Ast.Package)] = GenDarReader(ArchiveDecoder)

  val UniversalArchiveReader: GenUniversalArchiveReader[ArchivePayload] =
    new GenUniversalArchiveReader(ArchiveReader)
  val UniversalArchiveDecoder: GenUniversalArchiveReader[(PackageId, Ast.Package)] =
    new GenUniversalArchiveReader(ArchiveDecoder)

  @throws[Error]
  def packageInfo(archive: DamlLf.Archive): language.util.PackageInfo =
    new language.util.PackageInfo(Map(Decode.assertDecodeArchive(archive)))

}
