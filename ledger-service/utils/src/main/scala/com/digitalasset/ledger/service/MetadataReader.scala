// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.service

import java.io.File

import com.daml.lf.archive.{Dar, DarReader}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.iface
import com.daml.daml_lf_dev.DamlLf
import com.daml.util.ExceptionOps._
import scalaz.std.list._
import scalaz.syntax.traverse._
import scalaz.{Show, \/}

object MetadataReader {

  case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"MetadataReader.Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  type LfMetadata = Map[Ref.PackageId, iface.Interface]

  def readFromDar(darFile: File): Error \/ LfMetadata =
    for {
      dar <- \/.fromEither(
        DarReader().readArchiveFromFile(darFile).toEither
      ).leftMap(e => Error('readFromDar, e.description))

      packageStore <- decodePackageStoreFromDar(dar)

    } yield packageStore

  private def decodePackageStoreFromDar(
      dar: Dar[(Ref.PackageId, DamlLf.ArchivePayload)]): Error \/ LfMetadata = {

    dar.all
      .traverseU { a =>
        decodeInterfaceFromArchive(a).map(x => a._1 -> x): Error \/ (Ref.PackageId, iface.Interface)
      }
      .map(_.toMap)
  }

  private def decodeInterfaceFromArchive(
      a: (Ref.PackageId, DamlLf.ArchivePayload)): Error \/ iface.Interface =
    \/.fromTryCatchNonFatal {
      iface.reader.InterfaceReader.readInterface(a)
    }.leftMap(e => Error('decodeInterfaceFromArchive, e.description))
      .flatMap {
        case (errors, out) =>
          if (errors.empty) {
            \/.right(out)
          } else {
            val errorMsg = s"Errors reading LF archive ${a._1: Ref.PackageId}:\n${errors.toString}"
            \/.left(Error('decodeInterfaceFromArchive, errorMsg))
          }
      }

  def typeLookup(metaData: LfMetadata)(id: Ref.Identifier): Option[iface.DefDataType.FWT] =
    for {
      iface <- metaData.get(id.packageId)
      ifaceType <- iface.typeDecls.get(id.qualifiedName)
    } yield ifaceType.`type`

  def typeByName(metaData: LfMetadata)(
      name: Ref.QualifiedName): Seq[(Ref.PackageId, iface.DefDataType.FWT)] =
    metaData.values
      .map(interface => interface.typeDecls.get(name).map(x => (interface.packageId, x.`type`)))
      .collect { case Some(x) => x }(collection.breakOut)

  def templateByName(metaData: LfMetadata)(
      name: Ref.QualifiedName): Seq[(PackageId, iface.InterfaceType.Template)] =
    metaData.values
      .map(interface => interface.typeDecls.get(name).map(x => (interface.packageId, x)))
      .collect {
        case Some((pId, x @ iface.InterfaceType.Template(_, _))) => (pId, x)
      }(collection.breakOut)
}
