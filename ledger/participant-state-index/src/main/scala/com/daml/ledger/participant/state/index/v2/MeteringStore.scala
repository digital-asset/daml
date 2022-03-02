// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ApplicationId
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext

import scala.concurrent.Future

trait MeteringStore {

  def getMeteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[Ref.ApplicationId],
  )(implicit loggingContext: LoggingContext): Future[ReportData]

}

object MeteringStore {

  case class ReportData(applicationData: Map[ApplicationId, Long], isFinal: Boolean)

  case class TransactionMetering(
      applicationId: Ref.ApplicationId,
      actionCount: Int,
      meteringTimestamp: Timestamp,
      ledgerOffset: Offset,
  )

  case class ParticipantMetering(
      applicationId: Ref.ApplicationId,
      from: Timestamp,
      to: Timestamp,
      actionCount: Int,
      ledgerOffset: Offset,
  )

}
