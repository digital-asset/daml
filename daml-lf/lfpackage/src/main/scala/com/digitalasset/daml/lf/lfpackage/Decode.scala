// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package lfpackage

import java.io.InputStream

import com.digitalasset.daml.lf.archive.{Reader, LanguageVersion}
import com.digitalasset.daml.lf.archive.LanguageMajorVersion._
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml_lf.DamlLf
import com.google.protobuf.CodedInputStream

sealed abstract class Decode extends archive.Reader[(PackageId, Package)] {
  import Decode._

  private[lfpackage] val decoders: PartialFunction[LanguageVersion, PayloadDecoder]

  override protected[this] def readArchivePayloadOfVersion(
      hash: PackageId,
      lf: DamlLf.ArchivePayload,
      version: LanguageVersion
  ): (PackageId, Package) = {
    val decoder =
      decoders.lift(version).getOrElse(throw ParseError(s"$version unsupported"))

    (hash, decoder.decoder.decodePackage(hash, decoder.extract(lf)))
  }
}

object Decode extends Decode {
  type ParseError = Reader.ParseError
  val ParseError = Reader.ParseError

  def damlLfCodedInputStreamFromBytes(
      payload: Array[Byte],
      recursionLimit: Int = PROTOBUF_RECURSION_LIMIT
  ): CodedInputStream =
    Reader.damlLfCodedInputStreamFromBytes(payload, recursionLimit)

  def damlLfCodedInputStream(
      is: InputStream,
      recursionLimit: Int = PROTOBUF_RECURSION_LIMIT): CodedInputStream =
    Reader.damlLfCodedInputStream(is, recursionLimit)

  /** inlined [[scalaz.ContravariantCoyoneda]]`[OfPackage, DamlLf.ArchivePayload]` */
  private[lfpackage] sealed abstract class PayloadDecoder {
    type I
    val extract: DamlLf.ArchivePayload => I
    val decoder: OfPackage[I]
  }

  private[lfpackage] object PayloadDecoder {
    def apply[I0](fi: OfPackage[I0])(k: DamlLf.ArchivePayload => I0): PayloadDecoder =
      new PayloadDecoder {
        type I = I0
        override val extract = k
        override val decoder = fi
      }
  }

  override private[lfpackage] val decoders: PartialFunction[LanguageVersion, PayloadDecoder] = {
    case LanguageVersion(V1, minor) if V1.supportedMinorVersions.contains(minor) =>
      PayloadDecoder(new DecodeV1(minor))(_.getDamlLf1)
  }

  private[lf] trait OfPackage[-Pkg] {
    def decodePackage(packageId: PackageId, lfPackage: Pkg): Package
  }

  private def identifierStart(c: Char) =
    'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || c == '$' || c == '_'

  private def identifierPart(c: Char): Boolean =
    identifierStart(c) || '0' <= c && c <= '9'

  def checkIdentifier(s: String): Unit = {
    if (s.isEmpty)
      throw Reader.ParseError("empty identifier")
    else if (!(identifierStart(s.head) && s.tail.forall(identifierPart)))
      throw Reader.ParseError(s"identifier $s contains invalid character")
  }

  private val decimalPattern = "[+-]*[0-9]{0,28}(\\.[0-9]{0,10})*".r.pattern
  def checkDecimal(s: String): Boolean =
    decimalPattern.matcher(s).matches()

}
