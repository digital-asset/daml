// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.packages

import java.io.File
import java.time.Instant

import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.lf.archive.Reader.ParseError
import com.daml.lf.archive.{DarReader, Decode}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.daml.daml_lf_dev.DamlLf.Archive
import org.slf4j.LoggerFactory
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

import scala.collection.immutable.Map
import scala.concurrent.Future
import scala.util.Try

object InMemoryPackageStore {
  def empty: InMemoryPackageStore = new InMemoryPackageStore(Map.empty, Map.empty, Map.empty)
}

case class InMemoryPackageStore(
    packageInfos: Map[PackageId, PackageDetails],
    packages: Map[PackageId, Ast.Package],
    archives: Map[PackageId, Archive]) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    Future.successful(listLfPackagesSync())

  def listLfPackagesSync(): Map[PackageId, PackageDetails] =
    packageInfos

  def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    Future.successful(getLfArchiveSync(packageId))

  def getLfArchiveSync(packageId: PackageId): Option[Archive] =
    archives.get(packageId)

  def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
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
        logger.debug(
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
