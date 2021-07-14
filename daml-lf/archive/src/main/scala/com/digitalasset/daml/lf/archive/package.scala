// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.daml_lf_dev.{DamlLf, DamlLf1}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{Ast, LanguageVersion}
import com.google.protobuf.CodedInputStream

package object archive {

  // This constant is introduced and used
  // to make serialization of nested data
  // possible otherwise complex models failed to deserialize.
  private val PROTOBUF_RECURSION_LIMIT: Int = 1000

  @deprecated("use Error", since = "1.16.0")
  val Errors = Error

  @deprecated("use Error.Parsing", since = "1.16.0")
  val ParseError = Error.Parsing

  // just set the recursion limit
  private[this] val Base: GenReader[CodedInputStream] =
    new GenReader[CodedInputStream]({ cos =>
      cos.setRecursionLimit(PROTOBUF_RECURSION_LIMIT)
      cos
    })

  val ArchiveParser: GenReader[DamlLf.Archive] =
    Base.andThen(DamlLf.Archive.parseFrom)
  val ArchiveReader: GenReader[ArchivePayload] =
    ArchiveParser.andThen(Reader.readArchive)
  val ArchiveDecoder: GenReader[(PackageId, Ast.Package)] =
    ArchiveReader.andThen(Decode.decodeArchivePayload(_))

  val ArchivePayloadParser: GenReader[DamlLf.ArchivePayload] =
    Base.andThen(DamlLf.ArchivePayload.parseFrom)
  def archivePayloadDecoder(hash: PackageId): GenReader[Ast.Package] =
    ArchivePayloadParser
      .andThen(Reader.readArchivePayload(hash, _))
      .andThen(Decode.decodeArchivePayload(_)._2)

  private[lf] def moduleDecoder(ver: LanguageVersion, pkgId: PackageId): GenReader[Ast.Module] =
    Base
      .andThen(DamlLf1.Package.parseFrom)
      .andThen(new DecodeV1(ver.minor).decodeScenarioModule(pkgId, _))

  val DarParser: GenDarReader[DamlLf.Archive] = new GenDarReaderImpl(ArchiveParser)
  val DarReader: GenDarReader[ArchivePayload] = new GenDarReaderImpl(ArchiveReader)
  val DarDecoder: GenDarReader[(PackageId, Ast.Package)] = new GenDarReaderImpl(ArchiveDecoder)

  val UniversalArchiveReader: GenUniversalArchiveReader[ArchivePayload] =
    new GenUniversalArchiveReader(ArchiveReader)
  val UniversalArchiveDecoder: GenUniversalArchiveReader[(PackageId, Ast.Package)] =
    new GenUniversalArchiveReader(ArchiveDecoder)

}
