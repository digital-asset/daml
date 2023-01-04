// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.domain.{LedgerOffset, PackageEntry}
import com.daml.lf.data.Ref.PackageId
import com.daml.logging.LoggingContext

import scala.concurrent.Future

/** Serves as a backend to implement
  * PackageService and PackageManagementService.
  */
trait IndexPackagesService {
  def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[PackageId, PackageDetails]]

  def getLfArchive(
      packageId: PackageId
  )(implicit loggingContext: LoggingContext): Future[Option[Archive]]

  def packageEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  )(implicit loggingContext: LoggingContext): Source[PackageEntry, NotUsed]
}
