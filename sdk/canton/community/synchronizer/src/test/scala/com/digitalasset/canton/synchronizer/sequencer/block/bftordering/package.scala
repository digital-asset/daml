// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches}

package object bftordering {

  def emptySource[X](): Source[X, KillSwitch] =
    Source.empty.viaMat(KillSwitches.single)(Keep.right)

  def endpointToTestBftNodeId(endpoint: P2PEndpoint): BftNodeId =
    // Must be parseable as a valid sequencer ID, else the network output module will crash
    //  when generating peer statuses.
    SequencerNodeId.toBftNodeId(endpointToTestSequencerId(endpoint))

  def endpointToTestSequencerId(endpoint: P2PEndpoint): SequencerId =
    SequencerId(UniqueIdentifier.tryCreate("ns", s"${endpoint.address}_${endpoint.port}"))
}
