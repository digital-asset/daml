// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.service

import com.daml.ledger.api.domain.LedgerId
import com.daml.lf.archive
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.iface.reader.InterfaceReader
import com.daml.lf.iface.{DefDataType, Interface}
import com.daml.ledger.api.v1.package_service.GetPackageResponse
import com.daml.ledger.client.services.pkg.PackageClient
import com.daml.ledger.client.services.pkg.withoutledgerid.{PackageClient => LoosePackageClient}
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

  /** @return [[UpToDate]] if packages did not change
    */
  def loadPackageStoreUpdates(
      client: LoosePackageClient,
      token: Option[String],
      ledgerId: LedgerId,
  )(
      loadedPackageIds: Set[String]
  ): Future[Error \/ Option[PackageStore]] =
    for {
      newPackageIds <- client.listPackages(ledgerId, token).map(_.packageIds.toList)
      diffIds = newPackageIds.filterNot(loadedPackageIds): List[String] // keeping the order
      result <-
        if (diffIds.isEmpty) UpToDate
        else load[Option[PackageStore]](client, diffIds, ledgerId, token)
    } yield result

  /** @return [[UpToDate]] if packages did not change
    */
  def loadPackageStoreUpdates(client: PackageClient, token: Option[String])(
      loadedPackageIds: Set[String]
  ): Future[Error \/ Option[PackageStore]] =
    loadPackageStoreUpdates(client.it, token, client.ledgerId)(loadedPackageIds)

  private def load[PS >: Some[PackageStore]](
      client: LoosePackageClient,
      packageIds: List[String],
      ledgerId: LedgerId,
      token: Option[String],
  ): Future[Error \/ PS] =
    packageIds
      .traverse(client.getPackage(_, ledgerId, token))
      .map(createPackageStoreFromArchives)
      .map(_.map(Some(_)))

  private def createPackageStoreFromArchives(
      packageResponses: List[GetPackageResponse]
  ): Error \/ PackageStore = {
    packageResponses
      .traverse { packageResponse: GetPackageResponse =>
        decodeInterfaceFromPackageResponse(packageResponse).map { interface =>
          (interface.packageId, interface)
        }
      }
      .map(_.toMap)
  }

  private def decodeInterfaceFromPackageResponse(
      packageResponse: GetPackageResponse
  ): Error \/ Interface = {
    import packageResponse._
    \/.attempt {
      val payload = archive.ArchivePayloadParser.assertFromByteString(archivePayload)
      val (errors, out) =
        InterfaceReader.readInterface(PackageId.assertFromString(hash), payload)
      (if (!errors.empty) -\/("Errors reading LF archive:\n" + errors.toString)
       else \/-(out)): Error \/ Interface
    }(_.getLocalizedMessage).join
  }

  def damlLfTypeLookup(packageStore: () => PackageStore)(id: Identifier): Option[DefDataType.FWT] =
    for {
      iface <- packageStore().get(id.packageId)
      ifaceType <- iface.typeDecls.get(id.qualifiedName)
    } yield ifaceType.`type`
}
