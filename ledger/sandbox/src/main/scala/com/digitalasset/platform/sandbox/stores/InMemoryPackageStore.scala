// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.io.{File, FileOutputStream}
import java.time.Instant
import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.zip.ZipFile

import com.daml.ledger.participant.state.index.v2.{IndexPackagesService, PackageDetails}
import com.daml.ledger.participant.state.v2.{UploadDarRejectionReason, UploadDarResult}
import com.digitalasset.daml.lf.archive.Reader.ParseError
import com.digitalasset.daml.lf.archive.{DarReader, Decode}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.TryOps.Bracket.bracket
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml_lf.DamlLf.Archive
import org.slf4j.LoggerFactory
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class InMemoryPackageStore() extends IndexPackagesService {
  private val logger = LoggerFactory.getLogger(this.getClass)

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

  def uploadDar(
      knownSince: Instant,
      sourceDescription: String,
      packageBytes: Array[Byte]): CompletionStage[UploadDarResult] = this.synchronized {
    // TODO this should probably be asynchronous
    val file = File.createTempFile("put-package", ".dar")
    val result = bracket(Try(new FileOutputStream(file)))(fos => Try(fos.close()))
      .flatMap { fos =>
        Try(fos.write(packageBytes))
      }
      .map { _ =>
        putDarFile(knownSince, sourceDescription, file)
      }
    CompletableFuture.completedFuture(result match {
      case Success(Right(details @ _)) =>
        // TODO(FM) I'd like to include the details above but i get a strange error
        // about mismatching PackageId type
        UploadDarResult.Ok
      case Success(Left(err)) =>
        UploadDarResult.Rejected(UploadDarRejectionReason.InvalidPackage(err))
      case Failure(exception) =>
        UploadDarResult.Rejected(UploadDarRejectionReason.InvalidPackage(exception.getMessage))
    })
  }

  def putDarFile(
      knownSince: Instant,
      sourceDescription: String,
      file: File): Either[String, Map[PackageId, PackageDetails]] = this.synchronized {
    DarReader { case (size, x) => Try(Archive.parseFrom(x)).map(ar => (size, ar)) }
      .readArchive(new ZipFile(file))
      .fold(t => Left(s"Failed to parse DAR from $file: $t"), dar => Right(dar.all))
      .flatMap {
        _ traverseU {
          case (size, archive) =>
            try {
              Right((size, archive, Decode.decodeArchive(archive)._2))
            } catch {
              case err: ParseError => Left(s"Could not parse archive ${archive.getHash}: $err")
            }
        }
      }
      .map { pkgs =>
        Map(pkgs map {
          case (size, archive, pkg) =>
            val pkgId = PackageId.assertFromString(archive.getHash)
            val details = PackageDetails(size, knownSince, sourceDescription)
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

        }: _*)
      }
  }
}

object InMemoryPackageStore {
  def apply(): InMemoryPackageStore = new InMemoryPackageStore()
}
