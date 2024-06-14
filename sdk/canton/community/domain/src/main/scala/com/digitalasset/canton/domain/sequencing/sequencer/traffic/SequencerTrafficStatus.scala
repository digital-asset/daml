// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.Member

final case class SequencerTrafficStatus(
    trafficStatesOrErrors: Map[Member, Either[String, TrafficState]]
) {
  lazy val trafficStates: Map[Member, TrafficState] = trafficStatesOrErrors.flatMap {
    case (member, Right(trafficState)) => Some(member -> trafficState)
    case _ => None
  }
}
