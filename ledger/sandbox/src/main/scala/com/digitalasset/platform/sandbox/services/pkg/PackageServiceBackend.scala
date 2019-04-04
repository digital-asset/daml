// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.pkg

import com.digitalasset.ledger.api.v1.package_service.GetPackageResponse

trait PackageServiceBackend {

  def installedPackages: Set[String]

  def getPackage(packageId: String): Option[GetPackageResponse]
}
