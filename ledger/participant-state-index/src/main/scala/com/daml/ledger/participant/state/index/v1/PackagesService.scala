// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  def listPackages(): Future[List[PackageId]]

  def isPackageRegistered(packageId: PackageId): Future[Boolean]

  def getPackage(packageId: PackageId): Future[Option[Archive]]
}
