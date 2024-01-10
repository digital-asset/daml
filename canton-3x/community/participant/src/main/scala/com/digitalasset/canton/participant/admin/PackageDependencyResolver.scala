// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.github.blemale.scaffeine.Scaffeine

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class PackageDependencyResolver(
    private[admin] val damlPackageStore: DamlPackageStore,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends NamedLogging
    with FlagCloseable {

  def packageDependencies(packages: List[PackageId]): EitherT[Future, PackageId, Set[PackageId]] =
    packages
      .parTraverse(pkgId => OptionT(dependencyCache.get(pkgId)).toRight(pkgId))
      .map(_.flatten.toSet -- packages)

  def getPackageDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageDescription]] = damlPackageStore.getPackageDescription(packageId)

  def clearPackagesNotPreviouslyFound(): Unit = {
    dependencyCache
      .synchronous()
      .asMap()
      // .filterInPlace modifies the cache removing all packageId's from the cache that didn't exist when
      // queried/cached previously.
      .filterInPlace { case (_, deps) => deps.isDefined }
      .discard
  }

  private val dependencyCache = Scaffeine()
    .maximumSize(10000)
    .expireAfterAccess(15.minutes)
    .buildAsyncFuture[PackageId, Option[Set[PackageId]]] { packageId =>
      loadPackageDependencies(packageId)(TraceContext.empty).value
        .map(
          // Turn a "package does not exist"-error to a None in the cache value
          _.toOption
        )
        .onShutdown(None)
    }

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
