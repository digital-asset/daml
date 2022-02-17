// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import scala.concurrent.Future
import MeteringStore._

trait MeteringStore {

  def getTransactionMetering(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[Ref.ApplicationId],
  )(implicit loggingContext: LoggingContext): Future[Vector[TransactionMetering]]

}

object MeteringStore {

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
