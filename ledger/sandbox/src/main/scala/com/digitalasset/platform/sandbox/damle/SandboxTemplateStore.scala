// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.damle

import com.daml.ledger.participant.state.index.v2.IndexPackagesService
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.sandbox.config.DamlPackageContainer

import scala.collection.breakOut
import scala.collection.immutable.Map
import scala.concurrent.Future

private class SandboxTemplateStore(packageContainer: DamlPackageContainer)
    extends IndexPackagesService {

  private val packages: Map[PackageId, Archive] =
    packageContainer.archives.map {
      case (_, archive) =>
        (PackageId.assertFromString(archive.getHash), archive)
    }(breakOut)

  override def listPackages(): Future[Set[PackageId]] =
    Future.successful(packages.keySet)

  override def getPackage(packageId: PackageId): Future[Option[Archive]] =
    Future.successful(packages.get(packageId))

}

object SandboxTemplateStore {
  def apply(packageContainer: DamlPackageContainer): IndexPackagesService =
    new SandboxTemplateStore(packageContainer)
}
