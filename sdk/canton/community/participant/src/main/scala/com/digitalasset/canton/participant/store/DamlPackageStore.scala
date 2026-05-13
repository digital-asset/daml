// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.OptionT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.PackageDescription
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.admin.PackageService.{
  Dar,
  DarDescription,
  DarMainPackageId,
}
import com.digitalasset.canton.participant.store.db.DbDamlPackageStore
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.PackageMetadata

import scala.concurrent.ExecutionContext

final case class PackageInfo(name: String255, version: String255)

object PackageInfo {

  def fromPackageMetadata(
      metadata: PackageMetadata
  ): Either[String, PackageInfo] =
    for {
      name <- String255.create(metadata.name)
      version <- String255.create(metadata.version.toString())
    } yield PackageInfo(name, version)
}

/** For storing and retrieving Daml packages and DARs.
  */
trait DamlPackageStore extends AutoCloseable { this: NamedLogging =>

  /** @param pkgs
    *   Daml packages to be stored
    * @param uploadedAt
    *   The timestamp at which the package has been persisted in the store
    * @param dar
    *   The DAR containing the packages
    * @return
    *   Future which gets completed when the packages are successfully stored.
    */
  def append(
      pkgs: List[(PackageInfo, DamlLf.Archive)],
      uploadedAt: CantonTimestamp,
      dar: PackageService.Dar,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Remove the package from the package store.
    */
  def removePackage(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** @param packageId
    *   The package id of the Daml package to be retrieved from the store.
    * @return
    *   Future that will contain an empty option if the package with the given id could not be found
    *   or an option with the archive (serialized version of the package) if it could be found.
    */
  def getPackage(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[DamlLf.Archive]]

  def getPackageDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, PackageDescription]

  /** @return
    *   yields descriptions of all persisted Daml packages
    */
  def listPackages(limit: Option[Int] = None)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[PackageDescription]]

  /** Get DAR by its main package ID
    * @param mainPackageId
    *   The main package-id of the DAR archive
    * @return
    *   Future that will contain an empty option if the DAR with the given package ID could not be
    *   found or an option with the DAR if it could be found.
    */
  def getDar(mainPackageId: DarMainPackageId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, Dar]

  /** Get the packages in the DAR by its main package ID
    * @param mainPackageId
    *   The main package ID of the DAR file
    * @return
    *   The package description of the given dar
    */
  def getPackageDescriptionsOfDar(mainPackageId: DarMainPackageId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, Seq[PackageDescription]]

  /** Return all DARs that reference a given package */
  def getPackageReferences(packageId: LfPackageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DarDescription]]

  /** Remove the DAR with the main package ID from the store */
  def removeDar(mainPackageId: DarMainPackageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** @return
    *   Future with sequence of DAR descriptors (ID and name)
    */
  def listDars(limit: Option[Int] = None)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DarDescription]]

  /** Find from `packages` a registered package that does not exist in any dar except perhaps
    * `removeDar`. This checks whether a DAR containing `packages` can be safely removed -- if
    * there's any package that would be left without a DAR then we won't remove the DAR.
    */
  def anyPackagePreventsDarRemoval(packages: Seq[PackageId], removeDar: DarDescription)(implicit
      tc: TraceContext
  ): OptionT[FutureUnlessShutdown, PackageId]

  /** Returns the package IDs from the set of `packages` that are only referenced by the provided
    * `dar`.
    */
  def determinePackagesExclusivelyInDar(packages: Seq[PackageId], dar: DarDescription)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Seq[PackageId]]

}

object DamlPackageStore {

  def apply(
      storage: Storage,
      futureSupervisor: FutureSupervisor,
      parameterConfig: ParticipantNodeParameters,
      exitOnFatalFailures: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ) =
    storage match {
      case _: MemoryStorage =>
        new InMemoryDamlPackageStore(loggerFactory)
      case pool: DbStorage =>
        new DbDamlPackageStore(
          pool,
          parameterConfig.processingTimeouts,
          futureSupervisor,
          exitOnFatalFailures = exitOnFatalFailures,
          loggerFactory,
        )
    }

  /** Read the package id from a archive. Despite different types both values should be ascii7
    * values so runtime errors on the conversion are not expected.
    */
  def readPackageId(pkg: DamlLf.Archive): PackageId = PackageId.assertFromString(pkg.getHash)
}
