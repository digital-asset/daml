// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package iface
package reader

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.daml_lf_dev.DamlLf
import com.google.protobuf.InvalidProtocolBufferException
import scalaz.\/

object DamlLfArchiveReader {

  def readPackage(lf: DamlLf.Archive): String \/ (Ref.PackageId, Ast.Package) =
    readPayload(lf) flatMap readPackage

  private def readPayload(lf: DamlLf.Archive): String \/ (Ref.PackageId, DamlLf.ArchivePayload) =
    \/.attempt(archive.Reader.decodeArchive(lf))(errorMessage)

  private[iface] def readPackage(
      pkgIdAndPayLoad: (Ref.PackageId, DamlLf.ArchivePayload)
  ): String \/ (Ref.PackageId, Ast.Package) = {
    val (pkgId, lf) = pkgIdAndPayLoad
    \/.attempt(
      new archive.Decode(onlySerializableDataDefs = true).readArchivePayload(pkgId, lf)
    )(errorMessage)
  }

  private def errorMessage(t: Throwable): String = t match {
    case x: InvalidProtocolBufferException => s"Cannot parse protocol message: ${x.getMessage}"
    case archive.Reader.ParseError(err) => s"Cannot parse archive: $err"
    case _ => s"Unexpected exception: ${t.getMessage}"
  }

}
