// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.ReportData
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.UserId
import com.digitalasset.daml.lf.data.Time.Timestamp

import scala.concurrent.Future

trait MeteringStore {

  def getMeteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      userId: Option[Ref.UserId],
  )(implicit loggingContext: LoggingContextWithTrace): Future[ReportData]

}

object MeteringStore {

  final case class ReportData(applicationData: Map[UserId, Long], isFinal: Boolean)

  final case class TransactionMetering(
      userId: Ref.UserId,
      actionCount: Int,
      meteringTimestamp: Timestamp,
      ledgerOffset: Offset,
  )

  final case class ParticipantMetering(
      userId: Ref.UserId,
      from: Timestamp,
      to: Timestamp,
      actionCount: Int,
      ledgerOffset: Option[Offset],
  )

}
