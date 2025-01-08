// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.topology.ParticipantId

private[reassignment] final case class UnassignmentRequestValidated(
    request: UnassignmentRequest,
    recipients: Set[ParticipantId],
)
