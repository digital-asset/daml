// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.config.{CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.topology.store.PackageDependencyResolverUS
import com.digitalasset.canton.tracing.{TraceContext, TracedScaffeine}
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class PackageDependencyResolver(
    private[admin] val damlPackageStore: DamlPackageStore,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    fetchPackageParallelism: PositiveInt = PositiveInt.tryCreate(8),
    packageDependencyCacheConfig: CacheConfig = CacheConfig(
      maximumSize = PositiveLong.tryCreate(10000),
      expireAfterAccess = config.NonNegativeFiniteDuration.ofMinutes(15L),
    ),
)(implicit
    ec: ExecutionContext
) extends NamedLogging
    with FlagCloseable
    with PackageDependencyResolverUS {

  private val dependencyCache =
    TracedScaffeine
      .buildTracedAsync[EitherT[FutureUnlessShutdown, PackageId, *], PackageId, Set[PackageId]](
        cache = packageDependencyCacheConfig.buildScaffeine(),
        loader = implicit tc => loadPackageDependencies _,
      )(logger)

  def packageDependencies(packages: List[PackageId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
    packages.parTraverse(dependencyCache.get).map(_.view.flatten.toSet -- packages)

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
        directDependenciesByPackage <- MonadUtil.parTraverseWithLimit(
          fetchPackageParallelism.value
        )(packageIds) { packageId =>
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
                      com.daml.lf.archive.Decode.assertDecodeArchive(pckg)._2.directDeps
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

  override def onClosed(): Unit = Lifecycle.close(damlPackageStore)(logger)
}
