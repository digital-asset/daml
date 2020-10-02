// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.ledger.participant.state.v1.ParticipantId

package object logging {

  def participantId(id: ParticipantId): (String, String) = "participantId" -> id

}
