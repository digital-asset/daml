// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}

final case class ParticipantVettedPackages(
    packages: Seq[VettedPackage],
    participantId: ParticipantId,
    synchronizerId: SynchronizerId,
    serial: PositiveInt,
)
