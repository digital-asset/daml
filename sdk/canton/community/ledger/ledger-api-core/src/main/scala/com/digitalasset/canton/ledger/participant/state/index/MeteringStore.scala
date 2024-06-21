// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.ApplicationId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.ReportData
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

trait MeteringStore {

  def getMeteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[Ref.ApplicationId],
  )(implicit loggingContext: LoggingContextWithTrace): Future[ReportData]

}

object MeteringStore {

  final case class ReportData(applicationData: Map[ApplicationId, Long], isFinal: Boolean)

  final case class TransactionMetering(
      applicationId: Ref.ApplicationId,
      actionCount: Int,
      meteringTimestamp: Timestamp,
      ledgerOffset: Offset,
  )

  final case class ParticipantMetering(
      applicationId: Ref.ApplicationId,
      from: Timestamp,
      to: Timestamp,
      actionCount: Int,
      ledgerOffset: Offset,
  )

}
