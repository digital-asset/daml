// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.daml.ledger.api.v2.update_service.{GetUpdateResponse, GetUpdatesResponse}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.UpdateFormat
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateService]]
  */
trait IndexUpdateService extends LedgerEndService {
  def updates(
      begin: Option[Offset],
      endAt: Option[Offset],
      updateFormat: UpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdatesResponse, NotUsed]

  def getUpdateBy(
      lookupKey: LookupKey,
      updateFormat: UpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetUpdateResponse]]

  def latestPrunedOffset()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]]
}
