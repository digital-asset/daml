// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.OptionT
import com.daml.lf2.archive.daml_lf_dev.DamlLf
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.store.db.DbDamlPackageStore
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** For storing and retrieving Daml packages and DARs.
  */
trait DamlPackageStore extends AutoCloseable { this: NamedLogging =>

  /** @param pkgs Daml packages to be stored
    * @param dar The DAR containing the packages
    * @return Future which gets completed when the packages are successfully stored.
    */
  def append(
      pkgs: List[DamlLf.Archive],
      sourceDescription: String256M,
      dar: Option[PackageService.Dar],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Remove the package from the package store.
    */
  def removePackage(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** @param packageId The package id of the Daml package to be retrieved from the store.
    * @return Future that will contain an empty option if the package with the given id
    *         could not be found or an option with the archive (serialized version of the package) if it could be found.
    */
  def getPackage(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[DamlLf.Archive]]

  def getPackageDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageDescription]]

  /** @return yields descriptions of all persisted Daml packages
    */
  def listPackages(limit: Option[Int] = None)(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageDescription]]

  /** Get DAR by hash
    * @param hash The hash of the DAR file
    * @return Future that will contain an empty option if the DAR with the given hash
    *         could not be found or an option with the DAR if it could be found.
    */
  def getDar(hash: Hash)(implicit traceContext: TraceContext): Future[Option[Dar]]

  /** Remove the DAR with hash `hash` from the store */
  def removeDar(hash: Hash)(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** @return Future with sequence of DAR descriptors (hash and name)
    */
  def listDars(limit: Option[Int] = None)(implicit
      traceContext: TraceContext
  ): Future[Seq[DarDescriptor]]

  /** Find from `packages` a registered package that does not exist in any dar except perhaps `removeDar`.
    * This checks whether a DAR containing `packages` can be safely removed -- if there's any package that would be
    * left without a DAR then we won't remove the DAR.
    */
  def anyPackagePreventsDarRemoval(packages: Seq[PackageId], removeDar: DarDescriptor)(implicit
      tc: TraceContext
  ): OptionT[Future, PackageId]

  /** Returns the package IDs from the set of `packages` that are only referenced by the
    * provided `dar`.
    */
  def determinePackagesExclusivelyInDar(packages: Seq[PackageId], dar: DarDescriptor)(implicit
      tc: TraceContext
  ): Future[Seq[PackageId]]
}

object DamlPackageStore {

  def apply(
      storage: Storage,
      futureSupervisor: FutureSupervisor,
      parameterConfig: ParticipantNodeParameters,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ) = {
    storage match {
      case _: MemoryStorage =>
        new InMemoryDamlPackageStore(loggerFactory)
      case pool: DbStorage =>
        new DbDamlPackageStore(
          parameterConfig.batchingConfig.maxItemsInSqlClause,
          pool,
          parameterConfig.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )
    }
  }

  /** Read the package id from a archive.
    * Despite different types both values should be ascii7 values
    * so runtime errors on the conversion are not expected.
    */
  def readPackageId(pkg: DamlLf.Archive): PackageId = PackageId.assertFromString(pkg.getHash)
}
