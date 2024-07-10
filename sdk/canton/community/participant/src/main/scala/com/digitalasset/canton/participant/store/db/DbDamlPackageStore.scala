// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.OptionT
import com.daml.daml_lf_dev.DamlLf
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.{
  DarName,
  setParameterLengthLimitedString,
}
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String255}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.db.DbDamlPackageStore.{DamlPackage, DarRecord}
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.DbAction.WriteOnly
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SimpleExecutionQueue
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.{ExecutionContext, Future}

class DbDamlPackageStore(
    maxContractIdSqlInListSize: PositiveNumeric[Int],
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
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
      dar: DarDescriptor,
      sourceDescription: String255,
  )(implicit traceContext: TraceContext): DbAction.All[Unit] = {
    val insertToDamlPackages = {
      val sql = storage.profile match {
        case _: DbStorage.Profile.H2 =>
          """merge
            |  into par_daml_packages
            |  using (
            |    select
            |      cast(? as varchar(300)) as package_id,
            |      cast(? as varchar) as source_description,
            |    from dual
            |  ) as excluded
            |  on (par_daml_packages.package_id = excluded.package_id)
            |  when not matched then
            |    insert (package_id, data, source_description, uploaded_at, package_size)
            |    values (excluded.package_id, ?, excluded.source_description, ?, ?)
            |  when matched and ? then
            |    update set
            |      source_description = excluded.source_description""".stripMargin
        case _: DbStorage.Profile.Oracle =>
          """merge /*+ INDEX ( par_daml_packages (package_id) ) */
            |  into par_daml_packages
            |  using (
            |    select
            |      ? as package_id,
            |      ? as source_description
            |    from dual
            |  ) excluded
            |  on (par_daml_packages.package_id = excluded.package_id)
            |  when not matched then
            |    insert (package_id, data, source_description, uploaded_at, package_size)
            |    values (excluded.package_id, ?, excluded.source_description, ?, ?)
            |  when matched then
            |    update set
            |      source_description = excluded.source_description
            |      where ? = 1""".stripMargin // Strangely (or not), it looks like Oracle does not have a Boolean type...
        case _: DbStorage.Profile.Postgres =>
          """insert
              |  into par_daml_packages (package_id, source_description, data, uploaded_at, package_size)
              |  values (?, ?, ?, ?, ?)
              |  on conflict (package_id) do
              |    update set source_description = excluded.source_description
              |    where ?""".stripMargin
      }

      DbStorage.bulkOperation_(sql, pkgs, storage.profile) { pp => pkg =>
        pp >> pkg.packageId
        pp >> (if (sourceDescription.nonEmpty) sourceDescription
               else String255.tryCreate("default"))
        pp >> pkg.data
        pp >> pkg.uploadedAt
        pp >> pkg.packageSize
        pp >> sourceDescription.nonEmpty
      }
    }

    val insertToDarPackages = {
      val sql = storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          """merge /*+ INDEX ( dar_packages (dar_hash_hex package_id) ) */
            |  into par_dar_packages
            |  using (
            |    select
            |      ? as dar_hash_hex,
            |      ? package_id
            |    from dual
            |  ) excluded
            |  on (dar_packages.dar_hash_hex = excluded.dar_hash_hex and dar_packages.package_id = excluded.package_id)
            |  when not matched then
            |    insert (dar_hash_hex, package_id)
            |    values (excluded.dar_hash_hex, excluded.package_id)""".stripMargin
        case _ =>
          """insert into par_dar_packages (dar_hash_hex, package_id)
            |  values (?, ?)
            |  on conflict do
            |    nothing""".stripMargin
      }

      DbStorage.bulkOperation_(sql, pkgs, storage.profile) { pp => pkg =>
        pp >> (dar.hash.toLengthLimitedHexString: LengthLimitedString)
        pp >> pkg.packageId
      }
    }

    insertToDamlPackages.andThen(insertToDarPackages)
  }

  override def append(
      pkgs: List[DamlLf.Archive],
      uploadedAt: CantonTimestamp,
      sourceDescription: String255,
      dar: PackageService.Dar,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {

    val insertPkgs = insertOrUpdatePackages(
      pkgs.map(pkg =>
        DamlPackage(readPackageId(pkg), pkg.toByteArray, pkg.getPayload.size(), uploadedAt)
      ),
      dar.descriptor,
      sourceDescription,
    )

    val writeDar: List[WriteOnly[Int]] = List(appendToDarStore(dar))

    // Combine all the operations into a single transaction to avoid partial insertions.
    val writeDarAndPackages = DBIO
      .seq(writeDar :+ insertPkgs: _*)
      .transactionally

    val desc = "append Daml LF archive"
    writeQueue.execute(
      storage
        .queryAndUpdate(
          action = writeDarAndPackages,
          operationName = s"${this.getClass}: " + desc,
        ),
      desc,
    )
  }

  private def appendToDarStore(dar: Dar) = {
    val hash = dar.descriptor.hash
    val record = DarRecord(hash, dar.bytes, dar.descriptor.name)
    insertOrUpdateDar(record)
  }

  override def getPackage(
      packageId: PackageId
  )(implicit traceContext: TraceContext): Future[Option[DamlLf.Archive]] =
    storage.querySingle(exists(packageId), functionFullName).value

  override def getPackageDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageDescription]] = {
    storage
      .querySingle(
        sql"select package_id, source_description, uploaded_at, package_size from par_daml_packages where package_id = $packageId"
          .as[PackageDescription]
          .headOption,
        functionFullName,
      )
      .value
  }

  override def listPackages(
      limit: Option[Int]
  )(implicit traceContext: TraceContext): Future[Seq[PackageDescription]] =
    storage.query(
      sql"select package_id, source_description, uploaded_at, package_size from par_daml_packages #${limit
          .fold("")(storage.limit(_))}"
        .as[PackageDescription],
      functionFullName,
    )

  override def removePackage(
      packageId: PackageId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Removing package $packageId")

    writeQueue.execute(
      storage.update_(
        sqlu"""delete from par_daml_packages where package_id = $packageId """,
        functionFullName,
      ),
      functionFullName,
    )
  }

  private def packagesNotInAnyOtherDarsQuery(
      nonEmptyPackages: NonEmpty[Seq[PackageId]],
      darHash: Hash,
      limit: Option[Int],
  )(implicit traceContext: TraceContext) = {
    import DbStorage.Implicits.BuilderChain.*
    import com.digitalasset.canton.resource.DbStorage.Implicits.getResultPackageId

    val limitClause = limit.map(l => sql"#${storage.limit(l)}").getOrElse(sql"")

    val queryActions = DbStorage
      .toInClauses_(
        field = "package_id",
        values = nonEmptyPackages,
        maxContractIdSqlInListSize,
      )
      .map { inStatement =>
        (sql"""
                  select package_id
                  from par_dar_packages remove_candidates
                  where
                  """ ++ inStatement ++
          sql"""
                  and not exists (
                    select package_id
                    from par_dar_packages other_dars
                    where
                      remove_candidates.package_id = other_dars.package_id
                      and dar_hash_hex != ${darHash.toLengthLimitedHexString}
                  )""" ++ limitClause).as[LfPackageId]
      }

    storage.sequentialQueryAndCombine(queryActions, functionFullName).map(_.toSeq)
  }

  override def anyPackagePreventsDarRemoval(packages: Seq[PackageId], removeDar: DarDescriptor)(
      implicit tc: TraceContext
  ): OptionT[Future, PackageId] = {
    NonEmpty
      .from(packages)
      .fold(OptionT.none[Future, PackageId])(pkgs =>
        OptionT(
          packagesNotInAnyOtherDarsQuery(pkgs, removeDar.hash, limit = Some(1)).map(_.headOption)
        )
      )
  }

  override def determinePackagesExclusivelyInDar(
      packages: Seq[PackageId],
      dar: DarDescriptor,
  )(implicit
      tc: TraceContext
  ): Future[Seq[PackageId]] = {
    NonEmpty
      .from(packages)
      .fold(Future.successful(Seq.empty[PackageId]))(
        packagesNotInAnyOtherDarsQuery(_, dar.hash, limit = None)
      )
  }

  override def getDar(
      hash: Hash
  )(implicit traceContext: TraceContext): Future[Option[Dar]] =
    storage.querySingle(existing(hash), functionFullName).value

  override def listDars(
      limit: Option[Int]
  )(implicit traceContext: TraceContext): Future[Seq[DarDescriptor]] = {
    val query = limit match {
      case None => sql"select hash, name from par_dars".as[DarDescriptor]
      case Some(amount) =>
        sql"select hash, name from par_dars #${storage.limit(amount)}".as[DarDescriptor]
    }
    storage.query(query, functionFullName)
  }

  private def existing(hash: Hash): DbAction.ReadOnly[Option[Dar]] =
    sql"select hash, name, data from par_dars where hash_hex = ${hash.toLengthLimitedHexString}"
      .as[Dar]
      .headOption

  private def insertOrUpdateDar(dar: DarRecord): DbAction.WriteOnly[Int] =
    storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"merge into par_dars (hash_hex, hash, data, name) values (${dar.hash.toLengthLimitedHexString}, ${dar.hash}, ${dar.data}, ${dar.name})"
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_dars (hash_hex, hash, data, name) values (${dar.hash.toLengthLimitedHexString},${dar.hash}, ${dar.data}, ${dar.name})
               on conflict (hash_hex) do nothing"""
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
                /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( par_dars ( hash_hex ) ) */
                into par_dars (hash_hex, hash, data, name)
                values (${dar.hash.toLengthLimitedHexString}, ${dar.hash}, ${dar.data}, ${dar.name})
              """
    }

  override def removeDar(
      hash: Hash
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    writeQueue.execute(
      storage.update_(
        sqlu"""delete from par_dars where hash_hex = ${hash.toLengthLimitedHexString}""",
        functionFullName,
      ),
      functionFullName,
    )
  }

  override def onClosed(): Unit = {
    Lifecycle.close(writeQueue)(logger)
  }

}

object DbDamlPackageStore {
  private final case class DamlPackage(
      packageId: LfPackageId,
      data: Array[Byte],
      packageSize: Int,
      uploadedAt: CantonTimestamp,
  )

  private final case class DarRecord(hash: Hash, data: Array[Byte], name: DarName)
}
