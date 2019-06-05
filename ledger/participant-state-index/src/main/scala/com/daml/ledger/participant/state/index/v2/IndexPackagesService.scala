// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.daml.lf.lfpackage.Ast.Package

import scala.concurrent.Future

case class PackageDetails(size: Long, knownSince: Instant, sourceDescription: String)

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService]]
  */
trait IndexPackagesService {
  def listLfPackages(): Future[Map[PackageId, PackageDetails]]

  def getLfArchive(packageId: PackageId): Future[Option[Archive]]

  /** Like [[getLfArchive]], but already parsed. */
  def getLfPackage(packageId: PackageId): Future[Option[Package]]
}
