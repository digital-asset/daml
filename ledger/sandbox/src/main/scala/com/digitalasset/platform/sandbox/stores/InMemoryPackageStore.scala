// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.io.File
import java.time.Instant

import com.daml.ledger.participant.state.index.v2.{IndexPackagesService, PackageDetails}
import com.digitalasset.daml.lf.archive.Reader.ParseError
import com.digitalasset.daml.lf.archive.{DarReader, Decode}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml_lf.DamlLf.Archive
import org.slf4j.LoggerFactory

import scalaz.std.list._
import scalaz.std.either._
import scalaz.syntax.traverse._

import scala.collection.immutable.Map
import scala.concurrent.Future
import scala.util.Try

object InMemoryPackageStore {
  def empty(): InMemoryPackageStore = new InMemoryPackageStore(Map.empty, Map.empty, Map.empty)
}

case class InMemoryPackageStore(
    packageInfos: Map[PackageId, PackageDetails],
    packages: Map[PackageId, Ast.Package],
    archives: Map[PackageId, Archive])
    extends IndexPackagesService {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    Future.successful(listLfPackagesSync())

  def listLfPackagesSync(): Map[PackageId, PackageDetails] =
    packageInfos

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    Future.successful(getLfArchiveSync(packageId))

  def getLfArchiveSync(packageId: PackageId): Option[Archive] =
    archives.get(packageId)

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    Future.successful(getLfPackageSync(packageId))

  def getLfPackageSync(packageId: PackageId): Option[Ast.Package] =
    packages.get(packageId)

  def withPackages(
      knownSince: Instant,
      sourceDescription: Option[String],
      packages: List[Archive]): Either[String, InMemoryPackageStore] =
    addArchives(knownSince, sourceDescription, packages)

  def withDarFile(
      knownSince: Instant,
      sourceDescription: Option[String],
      file: File): Either[String, InMemoryPackageStore] = {
    val archivesTry = for {
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }.readArchiveFromFile(file)
    } yield dar.all

    for {
      archives <- archivesTry.toEither.left.map(t => s"Failed to parse DAR from $file: $t")
      packages <- addArchives(knownSince, sourceDescription, archives)
    } yield packages
  }

  private[InMemoryPackageStore] def addPackage(
      pkgId: PackageId,
      details: PackageDetails,
      archive: Archive,
      pkg: Ast.Package): InMemoryPackageStore =
    packageInfos.get(pkgId) match {
      case None =>
        new InMemoryPackageStore(
          packageInfos + (pkgId -> details),
          packages + (pkgId -> pkg),
          archives + (pkgId -> archive)
        )
      case Some(oldDetails) =>
        // Note: we are discarding the new metadata (size, known since, source description)
        logger.warn(
          s"Ignoring duplicate upload of package $pkgId. Existing package: $oldDetails, new package: $details")
        this
    }

  private def addArchives(
      knownSince: Instant,
      sourceDescription: Option[String],
      archives: List[Archive]
  ): Either[String, InMemoryPackageStore] =
    archives
      .map(archive =>
        try {
          Right((archive, Decode.decodeArchive(archive)._2))
        } catch {
          case err: ParseError => Left(s"Could not parse archive ${archive.getHash}: $err")
      })
      .sequenceU
      .map(pkgs =>
        pkgs.foldLeft(this) {
          case (store, (archive, pkg)) =>
            val pkgId = PackageId.assertFromString(archive.getHash)
            val details =
              PackageDetails(archive.getPayload.size.toLong, knownSince, sourceDescription)
            store.addPackage(pkgId, details, archive, pkg)
      })

}
