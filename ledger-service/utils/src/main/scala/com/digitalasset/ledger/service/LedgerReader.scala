// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.service

import com.digitalasset.daml.lf.archive.Reader
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId}
import com.digitalasset.daml.lf.iface.reader.InterfaceReader
import com.digitalasset.daml.lf.iface.{DefDataType, Interface}
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.v1.package_service.GetPackageResponse
import com.digitalasset.ledger.client.services.pkg.PackageClient
import scalaz.Scalaz._
import scalaz._

import scala.collection.immutable.Map
import scala.concurrent.Future

object LedgerReader {

  type Error = String

  // PackageId -> Interface
  type PackageStore = Map[String, Interface]

  val UpToDate: Future[Error \/ Option[PackageStore]] =
    Future.successful(\/-(None))

  // FIXME Find a more suitable execution context for these helpers
  import scala.concurrent.ExecutionContext.Implicits.global

  /**
    * @return [[UpToDate]] if packages did not change
    */
  def loadPackageStoreUpdates(client: PackageClient, token: Option[String])(
      loadedPackageIds: Set[String]): Future[Error \/ Option[PackageStore]] =
    for {
      newPackageIds <- client.listPackages(token).map(_.packageIds.toList)
      diffIds = newPackageIds.filterNot(loadedPackageIds): List[String] // keeping the order
      result <- if (diffIds.isEmpty) UpToDate
      else load(client, diffIds, token)
    } yield result

  private def load(
      client: PackageClient,
      packageIds: List[String],
      token: Option[String]): Future[Error \/ Some[PackageStore]] =
    packageIds
      .traverse(client.getPackage(_, token))
      .map(createPackageStoreFromArchives)
      .map(_.map(Some(_)))

  private def createPackageStoreFromArchives(
      packageResponses: List[GetPackageResponse]): Error \/ PackageStore = {
    packageResponses
      .traverseU { packageResponse: GetPackageResponse =>
        decodeInterfaceFromPackageResponse(packageResponse).map { interface =>
          (interface.packageId, interface)
        }
      }
      .map(_.toMap)
  }

  private def decodeInterfaceFromPackageResponse(
      packageResponse: GetPackageResponse): Error \/ Interface = {
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

  def damlLfTypeLookup(packageStore: () => PackageStore)(id: Identifier): Option[DefDataType.FWT] =
    for {
      iface <- packageStore().get(id.packageId.toString)
      ifaceType <- iface.typeDecls.get(id.qualifiedName)
    } yield ifaceType.`type`
}
