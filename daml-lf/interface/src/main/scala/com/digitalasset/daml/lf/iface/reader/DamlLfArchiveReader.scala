// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package iface
package reader

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.{ArchivePayload, Decode, Reader}
import com.google.protobuf.InvalidProtocolBufferException
import scalaz.\/

object DamlLfArchiveReader {

  def readPackage(lf: DamlLf.Archive): String \/ (Ref.PackageId, Ast.Package) =
    readPayload(lf) flatMap readPackage

  private[this] def readPayload(lf: DamlLf.Archive): String \/ ArchivePayload =
    \/.attempt(Reader.readArchive(lf))(errorMessage)

  private[iface] def readPackage(
      payLoad: ArchivePayload
  ): String \/ (Ref.PackageId, Ast.Package) =
    \/.attempt(Decode.decodeArchivePayload(payLoad, onlySerializableDataDefs = true))(
      errorMessage
    )

  private def errorMessage(t: Throwable): String = t match {
    case x: InvalidProtocolBufferException => s"Cannot parse protocol message: ${x.getMessage}"
    case err: archive.Error => s"Cannot parse archive: $err"
    case _ => s"Unexpected exception: ${t.getMessage}"
  }

}
