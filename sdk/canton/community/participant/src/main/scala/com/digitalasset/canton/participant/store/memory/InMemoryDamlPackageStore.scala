// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Monoid
import cats.data.OptionT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.PackageDescription
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.admin.PackageService.{
  Dar,
  DarDescription,
  DarMainPackageId,
}
import com.digitalasset.canton.participant.store.{DamlPackageStore, PackageInfo}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.daml.lf.data.Ref.PackageId

import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class InMemoryDamlPackageStore(override protected val loggerFactory: NamedLoggerFactory)(implicit
    ec: ExecutionContext
) extends DamlPackageStore
    with NamedLogging {
  import DamlPackageStore.*

  private val pkgData
      : concurrent.Map[LfPackageId, (DamlLf.Archive, PackageInfo, CantonTimestamp, Int)] =
    new ConcurrentHashMap[LfPackageId, (DamlLf.Archive, PackageInfo, CantonTimestamp, Int)].asScala

  private val darData: concurrent.Map[DarMainPackageId, (Array[Byte], DarDescription)] =
    new ConcurrentHashMap[DarMainPackageId, (Array[Byte], DarDescription)].asScala

  private val darPackages: concurrent.Map[DarMainPackageId, Set[LfPackageId]] =
    new ConcurrentHashMap[DarMainPackageId, Set[LfPackageId]].asScala

  override def append(
      pkgs: List[(PackageInfo, DamlLf.Archive)],
      uploadedAt: CantonTimestamp,
      dar: PackageService.Dar,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    pkgs.foreach { case (pkgInfo, pkgArchive) =>
      val packageId = readPackageId(pkgArchive)
      val packageSize = pkgArchive.getPayload.size()
      pkgData.putIfAbsent(packageId, (pkgArchive, pkgInfo, uploadedAt, packageSize)).discard
    }

    darData.put(dar.descriptor.mainPackageId, (dar.bytes.clone(), dar.descriptor)).discard
    val hash = dar.descriptor.mainPackageId
    val pkgS = pkgs.view.map { case (_, archive) => readPackageId(archive) }.toSet
    darPackages.updateWith(hash)(optSet => Some(optSet.fold(pkgS)(_.union(pkgS)))).discard

    FutureUnlessShutdown.unit
  }

  override def getPackage(packageId: LfPackageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[DamlLf.Archive]] =
    FutureUnlessShutdown.pure(pkgData.get(packageId).map(_._1))

  override def getPackageDescription(
      packageId: LfPackageId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, PackageDescription] =
    OptionT.fromOption[FutureUnlessShutdown](
      pkgData.get(packageId).map { case (_, info, uploadedAt, packageSize) =>
        PackageDescription(
          packageId = packageId,
          name = info.name,
          version = info.version,
          uploadedAt,
          packageSize,
        )
      }
    )

  override def getPackageReferences(
      packageId: LfPackageId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[DarDescription]] =
    FutureUnlessShutdown.pure(
      darPackages
        .filter { case (_, pkgs) => pkgs.contains(packageId) }
        .flatMap { case (k, _) =>
          darData.get(k).map(_._2)
        }
        .toSeq
    )

  private def toPackageDescription(
      packageId: LfPackageId,
      info: PackageInfo,
      uploadedAt: CantonTimestamp,
      packageSize: Int,
  ): PackageDescription =
    PackageDescription(
      packageId = packageId,
      name = info.name,
      version = info.version,
      uploadedAt,
      packageSize,
    )

  override def listPackages(
      limit: Option[Int]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[PackageDescription]] =
    FutureUnlessShutdown.pure(
      pkgData
        .take(limit.getOrElse(Int.MaxValue))
        .map { case (pid, (_, info, uploadedAt, packageSize)) =>
          toPackageDescription(pid, info, uploadedAt, packageSize)
        }
        .to(Seq)
    )

  override def removePackage(
      packageId: PackageId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    darPackages
      .mapValuesInPlace { case (_hash, packages) => packages - packageId }
      .filterInPlace { case (_hash, packages) => packages.nonEmpty }
      .discard

    pkgData.remove(packageId).discard

    FutureUnlessShutdown.unit
  }

  override def getDar(
      mainPackageId: DarMainPackageId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, Dar] =
    OptionT(FutureUnlessShutdown.pure(darData.get(mainPackageId).map { case (bytes, descriptor) =>
      Dar(descriptor, bytes.clone())
    }))

  override def getPackageDescriptionsOfDar(
      mainPackageId: DarMainPackageId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, Seq[PackageDescription]] =
    OptionT.fromOption[FutureUnlessShutdown](
      darPackages
        .get(mainPackageId)
        .map(
          _.flatMap(packageId =>
            pkgData.get(packageId).map { case (_, info, timestamp, size) =>
              toPackageDescription(packageId, info, timestamp, size)
            }
          ).toSeq
        )
    )

  override def listDars(
      limit: Option[Int]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[DarDescription]] =
    FutureUnlessShutdown.pure(
      darData
        .take(limit.getOrElse(Int.MaxValue))
        .map { case (_, (_, descriptor)) =>
          descriptor
        }
        .to(Seq)
    )

  override def anyPackagePreventsDarRemoval(packages: Seq[PackageId], removeDar: DarDescription)(
      implicit tc: TraceContext
  ): OptionT[FutureUnlessShutdown, PackageId] = {
    val known = packages.toSet.intersect(Monoid.combineAll(darPackages.toMap.values))
    val fromAllOtherDars =
      Monoid.combineAll(darPackages.toMap.removed(removeDar.mainPackageId).values)
    val withoutDar = known.diff(fromAllOtherDars).headOption
    OptionT.fromOption(withoutDar)
  }

  override def determinePackagesExclusivelyInDar(
      packages: Seq[PackageId],
      removeDar: DarDescription,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Seq[PackageId]] = {
    val packagesInOtherDars =
      Monoid.combineAll(darPackages.toMap.removed(removeDar.mainPackageId).values)
    val packagesNotInAnyOtherDars = packages.toSet.diff(packagesInOtherDars)
    FutureUnlessShutdown.pure(packagesNotInAnyOtherDars.toSeq)
  }

  override def removeDar(
      mainPackageId: DarMainPackageId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    darPackages.remove(mainPackageId).discard
    darData.remove(mainPackageId).discard
    FutureUnlessShutdown.unit
  }

  override def close(): Unit = ()
}
