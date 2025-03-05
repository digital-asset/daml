// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.OptionT
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.setParameterLengthLimitedString
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.PackageDescription
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.admin.PackageService.{
  Dar,
  DarDescription,
  DarMainPackageId,
}
import com.digitalasset.canton.participant.store.db.DbDamlPackageStore.DamlPackage
import com.digitalasset.canton.participant.store.{DamlPackageStore, PackageInfo}
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.DbAction.WriteOnly
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SimpleExecutionQueue
import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.ExecutionContext

class DbDamlPackageStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends DamlPackageStore
    with DbStore {

  import DamlPackageStore.*
  import DbStorage.Implicits.*
  import storage.api.*
  import storage.converters.*

  // writeQueue is used to protect against concurrent insertions and deletions to/from the `par_dars` or `par_daml_packages` tables,
  // which might otherwise data corruption or constraint violations.
  private val writeQueue = new SimpleExecutionQueue(
    "db-daml-package-store-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  private def exists(packageId: PackageId): DbAction.ReadOnly[Option[DamlLf.Archive]] =
    sql"select data from par_daml_packages where package_id = $packageId"
      .as[Array[Byte]]
      .headOption
      .map { mbData =>
        mbData.map(DamlLf.Archive.parseFrom)
      }

  private def insertOrUpdatePackages(
      pkgs: List[DamlPackage],
      dar: DarDescription,
  )(implicit traceContext: TraceContext): DbAction.All[Unit] = {
    val insertToDamlPackages = {
      val sql =
        """insert
              |  into par_daml_packages (package_id, data, name, version, uploaded_at, package_size)
              |  values (?, ?, ?, ?, ?, ?)
              |  on conflict do nothing
          """.stripMargin
      DbStorage.bulkOperation_(sql, pkgs, storage.profile) { pp => pkg =>
        {
          logger.debug(s"Storing package ${pkg.packageId} ${pkg.info}")
          pp >> pkg.packageId
          pp >> pkg.data
          pp >> pkg.info.name
          pp >> pkg.info.version
          pp >> pkg.uploadedAt
          pp >> pkg.packageSize
        }
      }
    }

    val insertToDarPackages = {
      val sql = """insert into par_dar_packages (main_package_id, package_id)
            |  values (?, ?)
            |  on conflict do
            |    nothing""".stripMargin

      DbStorage.bulkOperation_(sql, pkgs, storage.profile) { pp => pkg =>
        pp >> dar.mainPackageId.value
        pp >> pkg.packageId
      }
    }

    insertToDamlPackages.andThen(insertToDarPackages)
  }

  @SuppressWarnings(Array("org.wartremover.warts.AnyVal"))
  override def append(
      pkgs: List[(PackageInfo, DamlLf.Archive)],
      uploadedAt: CantonTimestamp,
      dar: PackageService.Dar,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {

    val insertPkgs = insertOrUpdatePackages(
      pkgs.map { case (info, pkg) =>
        DamlPackage(info, readPackageId(pkg), pkg.toByteArray, pkg.getPayload.size(), uploadedAt)
      },
      dar.descriptor,
    )

    val writeDar: List[WriteOnly[Int]] = List(insertOrUpdateDar(dar))

    // Combine all the operations into a single transaction to avoid partial insertions.
    val writeDarAndPackages = DBIO
      .seq(writeDar :+ insertPkgs: _*)
      .transactionally

    val desc = "append Daml LF archive"
    writeQueue.executeUS(
      storage
        .queryAndUpdate(
          action = writeDarAndPackages,
          operationName = s"${this.getClass}: " + desc,
        ),
      desc,
    )
  }

  override def getPackage(
      packageId: PackageId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[DamlLf.Archive]] =
    storage.querySingle(exists(packageId), functionFullName).value

  override def getPackageDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, PackageDescription] =
    storage
      .querySingle(
        sql"select package_id, name, version, uploaded_at, package_size from par_daml_packages where package_id = $packageId"
          .as[PackageDescription]
          .headOption,
        functionFullName,
      )

  override def listPackages(
      limit: Option[Int]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[PackageDescription]] =
    storage.query(
      sql"select package_id, name, version, uploaded_at, package_size from par_daml_packages #${limit
          .fold("")(storage.limit(_))}"
        .as[PackageDescription],
      functionFullName,
    )

  override def removePackage(
      packageId: PackageId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Removing package $packageId")

    writeQueue.executeUS(
      storage.update_(
        sqlu"""delete from par_daml_packages where package_id = $packageId """,
        functionFullName,
      ),
      functionFullName,
    )
  }

  private def packagesNotInAnyOtherDarsQuery(
      nonEmptyPackages: NonEmpty[Seq[PackageId]],
      mainPackageId: DarMainPackageId,
      limit: Option[Int],
  )(implicit traceContext: TraceContext) = {
    import DbStorage.Implicits.BuilderChain.*
    import com.digitalasset.canton.resource.DbStorage.Implicits.getResultPackageId

    val limitClause = limit.map(l => sql"#${storage.limit(l)}").getOrElse(sql"")

    val query = {
      val inClause = DbStorage.toInClause(field = "package_id", values = nonEmptyPackages)
      (sql"""select package_id
             from par_dar_packages remove_candidates
             where """ ++ inClause ++
        sql""" and not exists (
                 select package_id
                 from par_dar_packages other_dars
                 where
                   remove_candidates.package_id = other_dars.package_id
                   and main_package_id != ${mainPackageId.value}
               )""" ++ limitClause).as[LfPackageId]
    }

    storage.query(query, functionFullName).map(_.toSeq)
  }

  override def anyPackagePreventsDarRemoval(packages: Seq[PackageId], removeDar: DarDescription)(
      implicit tc: TraceContext
  ): OptionT[FutureUnlessShutdown, PackageId] =
    NonEmpty
      .from(packages)
      .fold(OptionT.none[FutureUnlessShutdown, PackageId])(pkgs =>
        OptionT(
          packagesNotInAnyOtherDarsQuery(pkgs, removeDar.mainPackageId, limit = Some(1))
            .map(_.headOption)
        )
      )

  override def determinePackagesExclusivelyInDar(
      packages: Seq[PackageId],
      dar: DarDescription,
  )(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Seq[PackageId]] =
    NonEmpty
      .from(packages)
      .fold(FutureUnlessShutdown.pure(Seq.empty[PackageId]))(
        packagesNotInAnyOtherDarsQuery(_, dar.mainPackageId, limit = None)
      )

  override def getDar(
      mainPackageId: DarMainPackageId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, Dar] =
    storage.querySingle(existing(mainPackageId), functionFullName)

  override def getPackageDescriptionsOfDar(
      mainPackageId: DarMainPackageId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, Seq[PackageDescription]] =
    OptionT(
      storage
        .query(
          sql"""select p.package_id, p.name, p.version, p.uploaded_at, p.package_size
              from par_daml_packages p, par_dar_packages r
              where p.package_id = r.package_id
                and r.main_package_id = ${mainPackageId.str}"""
            .as[PackageDescription],
          functionFullName,
        )
        .map(res => Option.when(res.nonEmpty)(res))
    )

  override def getPackageReferences(packageId: LfPackageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DarDescription]] =
    storage.query(
      sql"""
        SELECT d.main_package_id, d.description, d.name, d.version
          FROM par_dars d, par_dar_packages r
          WHERE r.main_package_id = d.main_package_id
            AND r.package_id = $packageId
        """.as[DarDescription],
      functionFullName,
    )

  override def listDars(
      limit: Option[Int]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[DarDescription]] = {
    val query = limit match {
      case None =>
        sql"select main_package_id, description, name, version from par_dars".as[DarDescription]
      case Some(amount) =>
        sql"select main_package_id, description, name, version from par_dars #${storage.limit(amount)}"
          .as[DarDescription]
    }
    storage.query(query, functionFullName)
  }

  private def existing(mainPackageId: DarMainPackageId): DbAction.ReadOnly[Option[Dar]] =
    sql"select main_package_id, description, name, version, data from par_dars where main_package_id = ${mainPackageId.str}"
      .as[Dar]
      .headOption

  private def insertOrUpdateDar(dar: Dar): DbAction.WriteOnly[Int] = {
    val pkgId = dar.descriptor.mainPackageId.unwrap
    val data = dar.bytes
    val description = dar.descriptor.description
    val name = dar.descriptor.name
    val version = dar.descriptor.version
    storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"merge into par_dars (main_package_id, data, description, name, version) values ($pkgId, $data, $description, $name, $version)"
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_dars (main_package_id, data, description, name, version) values ($pkgId, $data, $description, $name, $version)
               on conflict (main_package_id) do nothing"""
    }
  }

  override def removeDar(
      mainPackageId: DarMainPackageId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    writeQueue.executeUS(
      storage.update_(
        sqlu"""delete from par_dars where main_package_id = ${mainPackageId.str}""",
        functionFullName,
      ),
      functionFullName,
    )

  override def onClosed(): Unit =
    LifeCycle.close(writeQueue)(logger)

}

object DbDamlPackageStore {
  private final case class DamlPackage(
      info: PackageInfo,
      packageId: LfPackageId,
      data: Array[Byte],
      packageSize: Int,
      uploadedAt: CantonTimestamp,
  )
}
