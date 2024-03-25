// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.DefaultParticipantStateValues

object TestSubmissionTrackingData {

  lazy val default: SubmissionTrackingData =
    TransactionSubmissionTrackingData(
      DefaultParticipantStateValues.completionInfo(List.empty),
      TransactionSubmissionTrackingData.TimeoutCause,
      None,
      BaseTest.testedProtocolVersion,
    )
}
