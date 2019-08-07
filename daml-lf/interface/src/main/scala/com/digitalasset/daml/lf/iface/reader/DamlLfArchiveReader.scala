// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package iface
package reader

import com.digitalasset.daml.lf.archive.Reader.ParseError
import com.digitalasset.daml.lf.archive.{Decode, Reader => LFAR}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml_lf.DamlLf
import com.google.protobuf.InvalidProtocolBufferException
import scalaz.{-\/, \/, \/-}

import scala.util.Try

object DamlLfArchiveReader {

  def readPackage(lf: DamlLf.Archive): String \/ (Ref.PackageId, Ast.Package) =
    readPayload(lf) flatMap readPackage

  private[this] def readPayload(lf: DamlLf.Archive): String \/ (PackageId, DamlLf.ArchivePayload) =
    Try {
      val (tgid, arch) = LFAR.decodeArchive(lf)
      (tgid, arch)
    }.fold(e => -\/(errorMessage(e)), p => \/-(p))

  private def errorMessage(t: Throwable): String = t match {
    case x: InvalidProtocolBufferException => s"Cannot parse protocol message: ${x.getMessage}"
    case ParseError(err) => s"Cannot parse archive: $err"
    case _ => s"Unexpected exception: ${t.getMessage}"
  }

  private[iface] def readPackage(
      pkgIdAndPayLoad: (Ref.PackageId, DamlLf.ArchivePayload)
  ): String \/ (Ref.PackageId, Ast.Package) = {
    val (pkgId, lf) = pkgIdAndPayLoad
    Try(Decode.readArchivePayload(pkgId, lf)).fold(e => -\/(errorMessage(e)), pv => \/-(pv))
  }

}
