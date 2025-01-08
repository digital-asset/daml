// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.integrations

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.topology.Member

package object state {

  type MemberCounters = Map[Member, SequencerCounter]
}
