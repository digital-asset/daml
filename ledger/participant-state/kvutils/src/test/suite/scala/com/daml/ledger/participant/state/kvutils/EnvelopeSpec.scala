// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.{DamlKvutils => Proto}
import org.scalatest.{Matchers, WordSpec}

class EnvelopeSpec extends WordSpec with Matchers {
  "envelope" should {

    "be able to enclose and open" in {
      val subm = Proto.DamlSubmission.getDefaultInstance

      Envelope.open(Envelope.enclose(subm)) shouldEqual
        Right(Envelope.SubmissionMessage(subm))

      val logEntry = Proto.DamlLogEntry.getDefaultInstance
      Envelope.open(Envelope.enclose(logEntry)) shouldEqual
        Right(Envelope.LogEntryMessage(logEntry))

      val stateValue = Proto.DamlStateValue.getDefaultInstance
      Envelope.open(Envelope.enclose(stateValue)) shouldEqual
        Right(Envelope.StateValueMessage(stateValue))
    }
  }
}
