// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.iface
package reader

import com.digitalasset.daml.lf.archive.{Reader => LFAR}
import com.digitalasset.daml_lf.{DamlLf, DamlLf1}
import com.google.protobuf.InvalidProtocolBufferException
import scalaz.std.tuple._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/, \/-}

import scala.util.Try

object DamlLfV1ArchiveReader {
  def readPackage(lf: DamlLf.Archive): String \/ (PackageId, DamlLf1.Package) =
    readPayload(lf) flatMap (_ traverseU readPackage)

  private[this] def readPayload(lf: DamlLf.Archive): String \/ (PackageId, DamlLf.ArchivePayload) =
    Try {
      val (tgid, arch) = LFAR.decodeArchive(lf)
      (tgid, arch)
    }.fold(e => -\/(errorMessage(e)), p => \/-(p))

  private def errorMessage(t: Throwable): String = t match {
    case x: InvalidProtocolBufferException => s"Cannot parse protocol message: ${x.getMessage}"
    case _ => s"Unexpected exception: ${t.getMessage}"
  }

  private[iface] def readPackage(lf: DamlLf.ArchivePayload): String \/ DamlLf1.Package = {
    import DamlLf.ArchivePayload.{SumCase => SC}
    lf.getSumCase match {
      case SC.DAML_LF_1 => \/-(lf.getDamlLf1)
      case SC.DAML_LF_0 | SC.SUM_NOT_SET => -\/("LF other than v1 not supported")
    }
  }
}
