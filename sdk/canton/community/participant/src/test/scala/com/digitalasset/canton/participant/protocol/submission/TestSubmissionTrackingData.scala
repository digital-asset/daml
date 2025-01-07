// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.DefaultParticipantStateValues
import com.digitalasset.canton.topology.SynchronizerId

object TestSubmissionTrackingData {

  lazy val default: SubmissionTrackingData =
    TransactionSubmissionTrackingData(
      DefaultParticipantStateValues.completionInfo(List.empty),
      TransactionSubmissionTrackingData.TimeoutCause,
      SynchronizerId.tryFromString("da::default"),
      BaseTest.testedProtocolVersion,
    )
}
