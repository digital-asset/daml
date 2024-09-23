// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.topology.Member

package object statemanager {

  type MemberCounters = Map[Member, SequencerCounter]
}
