// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.damle

import com.digitalasset.daml_lf.DamlLf.{Archive, HashFunction}
import com.digitalasset.ledger.api.v1.package_service.HashFunction.{
  SHA256 => APISHA256,
  Unrecognized => APIUnrecognized
}
import com.digitalasset.ledger.api.v1.package_service.{
  GetPackageResponse,
  HashFunction => APIHashFunction
}
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.digitalasset.platform.sandbox.services.pkg.PackageServiceBackend

import scala.collection.breakOut
import scala.collection.immutable.Map

class SandboxTemplateStore(packageContainer: DamlPackageContainer) extends PackageServiceBackend {

  val packages: Map[String, GetPackageResponse] =
    packageContainer.archives.map { archive =>
      (archive.getHash, toGetPackageResponse(archive))
    }(breakOut)

  private def toGetPackageResponse(archive: Archive): GetPackageResponse = {
    val hashF: APIHashFunction = archive.getHashFunction match {
      case HashFunction.SHA256 => APISHA256
      case _ => APIUnrecognized(-1)
    }
    GetPackageResponse(hashF, archive.getPayload, archive.getHash)
  }

  override val installedPackages: Set[String] = packages.keySet

  override def getPackage(packageId: String): Option[GetPackageResponse] =
    packages.get(packageId)
}
