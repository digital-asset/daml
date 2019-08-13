// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.service

import java.io.File
import java.nio.file.Files

import com.digitalasset.daml.lf.data.Ref.{PackageId, Identifier}
import com.digitalasset.daml.lf.iface.reader.InterfaceReader
import com.digitalasset.daml.lf.iface.{Interface, DefDataType}
import com.digitalasset.daml.lf.archive.Reader
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.v1.package_service.GetPackageResponse
import com.digitalasset.ledger.client.services.pkg.PackageClient

import scala.concurrent.Future
import scala.collection.immutable.Map
import scalaz._
import Scalaz._

object LedgerReader {

  type PackageStore = Map[String, Interface]

  import scala.concurrent.ExecutionContext.Implicits.global

  def createPackageStore(packageClient: PackageClient): Future[String \/ PackageStore] = {
    for {
      packageIds <- packageClient.listPackages().map(_.packageIds)
      packageResponses <- Future
        .sequence(packageIds.map(packageClient.getPackage))
        .map(_.toList)
    } yield {
      createPackageStoreFromArchives(packageResponses)
    }
  }

  private def createPackageStoreFromArchives(
      packageResponses: List[GetPackageResponse]): String \/ PackageStore = {
    packageResponses
      .traverseU { packageResponse: GetPackageResponse =>
        decodeInterfaceFromPackageResponse(packageResponse).map { interface =>
          (interface.packageId, interface)
        }
      }
      .map(_.toMap)
  }

  def readArchiveFromFile(file: File): Archive = {
    DamlLf.Archive.parser().parseFrom(Files.readAllBytes(file.toPath))
  }

  def decodeInterfaceFromPackageResponse(
      packageResponse: GetPackageResponse): String \/ Interface = {
    import packageResponse._
    \/.fromTryCatchNonFatal {
      val cos = Reader.damlLfCodedInputStream(archivePayload.newInput)
      val payload = DamlLf.ArchivePayload.parseFrom(cos)
      val (errors, out) =
        InterfaceReader.readInterface(PackageId.assertFromString(hash) -> payload)
      if (!errors.empty) \/.left("Errors reading LF archive:\n" + errors.toString)
      else \/.right(out)
    }.leftMap(_.getLocalizedMessage).join
  }

  def damlLfTypeLookup(packageStore: PackageStore)(id: Identifier): Option[DefDataType.FWT] =
    for {
      iface <- packageStore.get(id.packageId.toString)
      ifaceType <- iface.typeDecls.get(id.qualifiedName)
    } yield ifaceType.`type`
}
