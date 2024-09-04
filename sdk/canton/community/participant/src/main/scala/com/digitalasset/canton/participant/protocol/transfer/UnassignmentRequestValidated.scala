// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.topology.ParticipantId

private[transfer] final case class UnassignmentRequestValidated(
    request: UnassignmentRequest,
    recipients: Set[ParticipantId],
)