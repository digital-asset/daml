// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.service

import java.io.File
import java.nio.file.Files

import com.digitalasset.daml.lf.data.Ref.{PackageId, Identifier}
import com.digitalasset.daml.lf.iface.reader.InterfaceReader
import com.digitalasset.daml.lf.iface.{Interface, DefDataType}
import com.digitalasset.daml.lf.archive.Reader
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.v1.package_service.GetPackageResponse
import com.digitalasset.ledger.client.services.pkg.PackageClient

import scala.concurrent.Future
import scala.collection.immutable.Map
import scalaz._
import Scalaz._

object LedgerReader {

  type Error = String

  // PackageId -> Interface
  type PackageStore = Map[String, Interface]

  val UpToDate: Future[Error \/ Option[PackageStore]] =
    Future.successful(\/-(None))

  import scala.concurrent.ExecutionContext.Implicits.global

  def createPackageStore(packageClient: PackageClient): Future[Error \/ PackageStore] =
    loadPackageStoreUpdates(packageClient)(Set.empty).map(x => x.map(_.getOrElse(Map.empty)))

  /**
    * @return [[UpToDate]] if packages did not change
    */
  def loadPackageStoreUpdates(client: PackageClient)(
      loadedPackageIds: Set[String]): Future[Error \/ Option[PackageStore]] =
    for {
      newPackageIds <- client.listPackages().map(_.packageIds.toList)
      diffIds = diff(newPackageIds, loadedPackageIds): List[String] // keeping the order
      result <- if (diffIds.isEmpty) UpToDate else load(client, diffIds).map(somePackageStore)
    } yield result

  // List.diff requires a Seq, I have got a Set
  private def diff[A](xs: List[A], ys: Set[A]): List[A] =
    xs.filter(x => !ys(x))

  private def somePackageStore(x: Error \/ PackageStore): Error \/ Option[PackageStore] =
    x.map(Some(_))

  private def load(client: PackageClient, packageIds: List[String]): Future[Error \/ PackageStore] =
    packageIds
      .traverse(client.getPackage)
      .map(createPackageStoreFromArchives)

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

  def readArchiveFromFile(file: File): Archive = {
    DamlLf.Archive.parser().parseFrom(Files.readAllBytes(file.toPath))
  }

  def decodeInterfaceFromPackageResponse(
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
