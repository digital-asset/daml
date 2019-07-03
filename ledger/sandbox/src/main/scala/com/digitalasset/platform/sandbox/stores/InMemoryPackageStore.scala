// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.io.File
import java.time.Instant
import java.util.zip.ZipFile

import com.daml.ledger.participant.state.index.v2.{IndexPackagesService, PackageDetails}
import com.daml.ledger.participant.state.v2.UploadPackagesResult
import com.digitalasset.daml.lf.archive.Reader.ParseError
import com.digitalasset.daml.lf.archive.{DarReader, Decode}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml_lf.DamlLf.Archive
import org.slf4j.LoggerFactory
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try

class InMemoryPackageStore() extends IndexPackagesService {
  private val logger = LoggerFactory.getLogger(this.getClass)

  //TODO: it's unnecessary to have mutable state in this class. Mutating operations could just create new instances of immutable Maps
  private val packageInfos: mutable.Map[PackageId, PackageDetails] = mutable.Map()
  private val packages: mutable.Map[PackageId, Ast.Package] = mutable.Map()
  private val archives: mutable.Map[PackageId, Archive] = mutable.Map()

  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    Future.successful(listLfPackagesSync())

  def listLfPackagesSync(): Map[PackageId, PackageDetails] = this.synchronized {
    Map() ++ packageInfos
  }

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    Future.successful(getLfArchiveSync(packageId))

  def getLfArchiveSync(packageId: PackageId): Option[Archive] = this.synchronized {
    archives.get(packageId)
  }

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    Future.successful(getLfPackageSync(packageId))

  def getLfPackageSync(packageId: PackageId): Option[Ast.Package] = this.synchronized {
    packages.get(packageId)
  }

  def uploadPackages(
      knownSince: Instant,
      sourceDescription: Option[String],
      packages: List[Archive]): UploadPackagesResult = this.synchronized {
    val result = addArchives(knownSince, sourceDescription, packages)
    result match {
      case Right(details @ _) =>
        // TODO(FM) I'd like to include the details above but i get a strange error
        // about mismatching PackageId type
        UploadPackagesResult.Ok
      case Left(err) =>
        UploadPackagesResult.InvalidPackage(err)
    }
  }

  private def addPackage(
      pkgId: PackageId,
      details: PackageDetails,
      archive: Archive,
      pkg: Ast.Package): (PackageId, PackageDetails) = this.synchronized {
    packageInfos.get(pkgId) match {
      case None =>
        packageInfos += (pkgId -> details)
        archives += (pkgId -> archive)
        packages += (pkgId -> pkg)
        (pkgId, details)
      case Some(oldDetails) =>
        // Note: we are discarding the new metadata (size, known since, source description)
        logger.warn(
          s"Ignoring duplicate upload of package $pkgId. Existing package: $oldDetails, new package: $details")
        (pkgId, oldDetails)
    }
  }

  private def addArchives(
      knownSince: Instant,
      sourceDescription: Option[String],
      archives: List[Archive]
  ): Either[String, Map[PackageId, PackageDetails]] = this.synchronized {
    archives
      .traverseU(archive =>
        try {
          Right((archive, Decode.decodeArchive(archive)._2))
        } catch {
          case err: ParseError => Left(s"Could not parse archive ${archive.getHash}: $err")
      })
      .map(pkgs =>
        Map(pkgs map {
          case (archive, pkg) =>
            val pkgId = PackageId.assertFromString(archive.getHash)
            val details =
              PackageDetails(archive.getPayload.size.toLong, knownSince, sourceDescription)
            addPackage(pkgId, details, archive, pkg)
        }: _*))
  }

  def putDarFile(
      knownSince: Instant,
      sourceDescription: Option[String],
      file: File): Either[String, Map[PackageId, PackageDetails]] = this.synchronized {
    val archivesTry = for {
      zipFile <- Try(new ZipFile(file))
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }.readArchive(zipFile)
    } yield dar.all

    for {
      archives <- archivesTry.toEither.left.map(t => s"Failed to parse DAR from $file: $t")
      packages <- addArchives(knownSince, sourceDescription, archives)
    } yield packages
  }
}

object InMemoryPackageStore {
  def empty: InMemoryPackageStore = new InMemoryPackageStore()
}
