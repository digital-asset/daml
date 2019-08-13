// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml_lf.DamlLf.Archive

import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService]]
  */
trait PackagesService {
  def listPackageDetails(): Future[Map[PackageId, PackageDetails]]

  def listPackages(): Future[Set[PackageId]]

  def getPackage(packageId: PackageId): Future[Option[Archive]]
}
