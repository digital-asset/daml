// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.lf.data.Ref

case class ParticipantName(value: String) extends AnyVal

object ParticipantName {
  def fromParticipantId(participantId: Ref.ParticipantId, shardName: Option[String] = None) =
    ParticipantName(participantId + shardName.map("-" + _).getOrElse(""))
}
