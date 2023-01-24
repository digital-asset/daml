// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.service

import com.daml.ledger.api.domain.LedgerId
import com.daml.lf.archive
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.lf.typesig.{DefDataType, PackageSignature}
import com.daml.ledger.api.v1.package_service.GetPackageResponse
import com.daml.ledger.client.services.pkg.PackageClient
import com.daml.ledger.client.services.pkg.withoutledgerid.{PackageClient => LoosePackageClient}
import com.daml.lf.data.ImmArray.ImmArraySeq
import scalaz.Scalaz._
import scalaz._

import scala.collection.immutable.Map
import scala.concurrent.{ExecutionContext, Future}

object LedgerReader {

  type Error = String

  // PackageId -> PackageSignature
  type PackageStore = Map[String, PackageSignature]

  val UpToDate: Future[Error \/ Option[PackageStore]] =
    Future.successful(\/-(None))

  /** @return [[UpToDate]] if packages did not change
    */
  def loadPackageStoreUpdates(
      client: LoosePackageClient,
      token: Option[String],
      ledgerId: LedgerId,
  )(
      loadedPackageIds: Set[String]
  )(implicit ec: ExecutionContext): Future[Error \/ Option[PackageStore]] =
    for {
      newPackageIds <- client.listPackages(ledgerId, token).map(_.packageIds.toList)
      diffIds = newPackageIds.filterNot(loadedPackageIds): List[String] // keeping the order
      result <-
        if (diffIds.isEmpty) UpToDate
        else load[Option[PackageStore]](client, diffIds, ledgerId, token)
    } yield result

  /** @return [[UpToDate]] if packages did not change
    */
  @deprecated("TODO #15922 unused?", since = "2.5.2")
  def loadPackageStoreUpdates(client: PackageClient, token: Option[String])(
      loadedPackageIds: Set[String]
  )(implicit ec: ExecutionContext): Future[Error \/ Option[PackageStore]] =
    loadPackageStoreUpdates(client.it, token, client.ledgerId)(loadedPackageIds)

  private val loadEC =
    concurrent.ExecutionContext fromExecutorService java.util.concurrent.Executors
      .newSingleThreadExecutor()

  private val loadCache = {
    import com.daml.caching.CaffeineCache, com.github.benmanes.caffeine.cache.Caffeine
    CaffeineCache[String, GetPackageResponse](
      Caffeine
        .newBuilder()
        .softValues()
        .expireAfterWrite(60, java.util.concurrent.TimeUnit.SECONDS),
      None,
    )
  }

  private def load[PS >: Some[PackageStore]](
      client: LoosePackageClient,
      packageIds: List[String],
      ledgerId: LedgerId,
      token: Option[String],
  ): Future[Error \/ PS] = {
    implicit val ec: ExecutionContext = loadEC
    packageIds
      .traverse(pkid =>
        loadCache
          .getIfPresent(pkid)
          .cata(
            Future.successful,
            client.getPackage(pkid, ledgerId, token).map { pkresp =>
              loadCache.put(pkid, pkresp)
              pkresp
            },
          )
      )
      .map(createPackageStoreFromArchives)
      .map(_.map(Some(_)))
  }

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
  ): Error \/ PackageSignature = {
    import packageResponse._
    \/.attempt {
      val payload = archive.ArchivePayloadParser.assertFromByteString(archivePayload)
      val (errors, out) =
        SignatureReader.readPackageSignature(PackageId.assertFromString(hash), payload)
      (if (!errors.empty) -\/("Errors reading LF archive:\n" + errors.toString)
       else \/-(out)): Error \/ PackageSignature
    }(_.getLocalizedMessage).join
  }

  def damlLfTypeLookup(
      packageStore: () => PackageStore
  )(id: Identifier): Option[DefDataType.FWT] = {
    val store = packageStore()

    store.get(id.packageId).flatMap { packageSignature =>
      packageSignature.typeDecls.get(id.qualifiedName).map(_.`type`).orElse {
        for {
          interface <- packageSignature.interfaces.get(id.qualifiedName)
          viewTypeId <- interface.viewType
          viewType <- PackageSignature.resolveInterfaceViewType(store).lift(viewTypeId)
        } yield DefDataType(ImmArraySeq(), viewType)
      }
    }
  }
}
