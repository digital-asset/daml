// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.ledger.api.domain.{LedgerOffset, PackageEntry}

import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * PackageService and PackageManagementService.
  */
trait IndexPackagesService {
  def listLfPackages(): Future[Map[PackageId, PackageDetails]]

  def getLfArchive(packageId: PackageId): Future[Option[Archive]]

  /** Like [[getLfArchive]], but already parsed. */
  def getLfPackage(packageId: PackageId): Future[Option[Package]]

  def packageEntries(startExclusive: LedgerOffset.Absolute): Source[PackageEntry, NotUsed]
}
