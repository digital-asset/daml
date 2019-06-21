// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.services.pkg.PackageClient

import scala.concurrent.{ExecutionContext, Future}

class PackageService(packageClient: PackageClient)(implicit ec: ExecutionContext) {
  def packageMapping(): Future[Map[(String, String), Identifier]] =
    for {
      packageIds <- packageClient.listPackages().map(_.packageIds)
    } yield ???

  private def buildMapping(packageId: String): Unit = {
//    packageClient.getPackage(packageId).map(
  }

}
