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
      loadCache: LoadCache,
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
        else load[Option[PackageStore]](client, loadCache, diffIds, ledgerId, token)
    } yield result

  /** @return [[UpToDate]] if packages did not change
    */
  @deprecated("TODO #15922 unused?", since = "2.5.2")
  def loadPackageStoreUpdates(client: PackageClient, loadCache: LoadCache, token: Option[String])(
      loadedPackageIds: Set[String]
  )(implicit ec: ExecutionContext): Future[Error \/ Option[PackageStore]] =
    loadPackageStoreUpdates(client.it, loadCache, token, client.ledgerId)(loadedPackageIds)

  final class LoadCache private () {
    import com.daml.caching.CaffeineCache, com.github.benmanes.caffeine.cache.Caffeine

    // This cache serves *concurrent* load requests, not *subsequent* requests;
    // once a request is complete, its records shouldn't be touched at all for
    // any requests that follow for the rest of the server lifetime, hence the
    // short timeout.  The timeout is chosen to allow concurrent contention to
    // resolve even in unideal execution situations with large package sets, but
    // short enough not to pointlessly cache for pkg reqs that do not overlap at
    // all.
    //
    // A hit indicates concurrent contention, so we actually want to *maximize
    // misses, not hits*, but the hitrate is really determined by the client's
    // request pattern, so there isn't anything you can really do about it on
    // the server configuration.  100% miss rate means no redundant work is
    // happening; it does not mean the server is being slower.
    private[LedgerReader] val cache = CaffeineCache[(LedgerId, String), GetPackageResponse](
      Caffeine
        .newBuilder()
        .softValues()
        .expireAfterWrite(60, java.util.concurrent.TimeUnit.SECONDS),
      None,
    )
  }

  object LoadCache {
    def freshCache(): LoadCache = new LoadCache()
  }

  private def load[PS >: Some[PackageStore]](
      client: LoosePackageClient,
      loadCache: LoadCache,
      packageIds: List[String],
      ledgerId: LedgerId,
      token: Option[String],
  )(implicit ec: ExecutionContext): Future[Error \/ PS] = {
    packageIds
      .traverse { pkid =>
        val ck = (ledgerId, pkid)
        loadCache.cache
          .getIfPresent(ck)
          .cata(
            { v => println("s11 hit"); Future.successful(v) },
            client.getPackage(pkid, ledgerId, token).map { pkresp =>
              println(
                loadCache.cache.getIfPresent(ck).cata(_ => "s11 granular contention", "s11 miss")
              )
              loadCache.cache.put(ck, pkresp)
              pkresp
            },
          )
      }
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
