// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.io.InputStream

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageMajorVersion._
import com.daml.lf.language.LanguageVersion
import com.daml.daml_lf_dev.DamlLf
import com.google.protobuf.CodedInputStream

sealed class Decode(onlySerializableDataDefs: Boolean)
    extends archive.Reader[(PackageId, Package)] {
  import Decode._

  private[lf] val decoders: PartialFunction[LanguageVersion, PayloadDecoder] = {
    case LanguageVersion(V1, minor) if V1.supportedMinorVersions.contains(minor) =>
      PayloadDecoder(new DecodeV1(minor))(_.getDamlLf1)
  }

  override protected[this] def readArchivePayloadOfVersion(
      hash: PackageId,
      lf: DamlLf.ArchivePayload,
      version: LanguageVersion
  ): (PackageId, Package) = {
    val decoder =
      decoders.lift(version).getOrElse(throw ParseError(s"$version unsupported"))

    (hash, decoder.decoder.decodePackage(hash, decoder.extract(lf), onlySerializableDataDefs))
  }
}

object Decode extends Decode(onlySerializableDataDefs = false) {
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
  private[lf] sealed abstract class PayloadDecoder {
    type I
    val extract: DamlLf.ArchivePayload => I
    val decoder: OfPackage[I]
  }

  private[archive] object PayloadDecoder {
    def apply[I0](fi: OfPackage[I0])(k: DamlLf.ArchivePayload => I0): PayloadDecoder =
      new PayloadDecoder {
        type I = I0
        override val extract = k
        override val decoder = fi
      }
  }

  private[lf] trait OfPackage[-Pkg] {
    type ProtoScenarioModule
    def protoScenarioModule(cis: CodedInputStream): ProtoScenarioModule
    @throws[ParseError]
    def decodePackage(
        packageId: PackageId,
        lfPackage: Pkg,
        onlySerializableDataDefs: Boolean = false): Package
    @throws[ParseError]
    def decodeScenarioModule(packageId: PackageId, lfModuleForScenario: ProtoScenarioModule): Module
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
