// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.topology.Member

package object statemanager {

  type MemberCounters = Map[Member, SequencerCounter]
  type MemberTimestamps = Map[Member, CantonTimestamp]
  type MemberSignedEvents = Map[Member, OrdinarySerializedEvent]
}
