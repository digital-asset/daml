// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.daml.lf2.archive.daml_lf_dev.DamlLf.Archive
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.ledger.api.domain.{PackageEntry, ParticipantOffset}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Serves as a backend to implement
  * PackageService and PackageManagementService.
  */
trait IndexPackagesService {
  def listLfPackages()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Map[PackageId, PackageDetails]]

  def getLfArchive(
      packageId: PackageId
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[Archive]]

  def packageEntries(
      startExclusive: Option[ParticipantOffset.Absolute]
  )(implicit loggingContext: LoggingContextWithTrace): Source[PackageEntry, NotUsed]
}
