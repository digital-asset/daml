// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageMajorVersion._
import com.daml.lf.language.LanguageVersion
import com.daml.daml_lf_dev.DamlLf
import com.google.protobuf.CodedInputStream

sealed class Decode(onlySerializableDataDefs: Boolean) {
  import Decode._

  private[lf] val decoders: PartialFunction[LanguageVersion, PayloadDecoder] = {
    case LanguageVersion(V1, minor) if V1.supportedMinorVersions.contains(minor) =>
      PayloadDecoder(new DecodeV1(minor))(_.getDamlLf1)
  }

  def decode(archive: DamlLf.Archive): (PackageId, Package) =
    decode(Reader().readArchive(archive))

  def decode(payload: ArchivePayload): (PackageId, Package) = {
    val decoder =
      decoders
        .lift(payload.version)
        .getOrElse(throw ParseError(s"${payload.version} unsupported"))
    (
      payload.pkgId,
      decoder.decoder.decodePackage(
        payload.pkgId,
        decoder.extract(payload.proto),
        onlySerializableDataDefs,
      ),
    )
  }
}

object Decode extends Decode(onlySerializableDataDefs = false) {

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
        onlySerializableDataDefs: Boolean = false,
    ): Package
    @throws[ParseError]
    def decodeScenarioModule(packageId: PackageId, lfModuleForScenario: ProtoScenarioModule): Module
  }

  private def identifierStart(c: Char) =
    'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || c == '$' || c == '_'

  private def identifierPart(c: Char): Boolean =
    identifierStart(c) || '0' <= c && c <= '9'

  def checkIdentifier(s: String): Unit = {
    if (s.isEmpty)
      throw ParseError("empty identifier")
    else if (!(identifierStart(s.head) && s.tail.forall(identifierPart)))
      throw ParseError(s"identifier $s contains invalid character")
  }

  private val decimalPattern = "[+-]*[0-9]{0,28}(\\.[0-9]{0,10})*".r.pattern
  def checkDecimal(s: String): Boolean =
    decimalPattern.matcher(s).matches()

}
