// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.topology.store.PackageDependencyResolverUS
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.github.blemale.scaffeine.Scaffeine

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class PackageDependencyResolver(
    val damlPackageStore: DamlPackageStore,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends NamedLogging
    with FlagCloseable
    with PackageDependencyResolverUS {

  private val dependencyCache: TracedAsyncLoadingCache[
    EitherT[FutureUnlessShutdown, PackageId, *],
    PackageId,
    Set[PackageId],
  ] = ScaffeineCache
    .buildTracedAsync[EitherT[FutureUnlessShutdown, PackageId, *], PackageId, Set[PackageId]](
      cache = Scaffeine().maximumSize(10000).expireAfterAccess(15.minutes).executor(ec.execute(_)),
      loader = implicit tc => loadPackageDependencies _,
      allLoader = None,
    )(logger, "dependencyCache")

  def packageDependencies(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
    dependencyCache.get(packageId).map(_ - packageId)

  def packageDependencies(packages: List[PackageId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
    packages
      .parTraverse(packageDependencies)
      .map(_.flatten.toSet -- packages)

  def getPackageDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageDescription]] = damlPackageStore.getPackageDescription(packageId)

  private def loadPackageDependencies(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] = {
    def computeDirectDependencies(
        packageIds: List[PackageId]
    ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
      for {
        directDependenciesByPackage <- packageIds.parTraverse { packageId =>
          for {
            pckg <- OptionT(
              performUnlessClosingF(functionFullName)(damlPackageStore.getPackage(packageId))
            )
              .toRight(packageId)
            directDependencies <- EitherT(
              performUnlessClosingF(functionFullName)(
                Future(
                  Either
                    .catchOnly[Exception](
                      com.digitalasset.daml.lf.archive.Decode
                        .assertDecodeArchive(pckg)
                        ._2
                        .directDeps
                    )
                    .leftMap { e =>
                      logger.error(
                        s"Failed to decode package with id $packageId while trying to determine dependencies",
                        e,
                      )
                      packageId
                    }
                )
              )
            )
          } yield directDependencies
        }
      } yield directDependenciesByPackage.reduceLeftOption(_ ++ _).getOrElse(Set.empty)

    def go(
        packageIds: List[PackageId],
        knownDependencies: Set[PackageId],
    ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
      if (packageIds.isEmpty) EitherT.rightT(knownDependencies)
      else {
        for {
          directDependencies <- computeDirectDependencies(packageIds)
          newlyDiscovered = directDependencies -- knownDependencies - packageId
          allDependencies <- go(newlyDiscovered.toList, knownDependencies ++ newlyDiscovered)
        } yield allDependencies
      }

    go(List(packageId), Set())

  }

  override def onClosed(): Unit = LifeCycle.close(damlPackageStore)(logger)
}
