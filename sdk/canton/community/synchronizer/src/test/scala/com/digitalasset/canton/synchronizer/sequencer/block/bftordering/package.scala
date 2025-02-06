// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches}

package object bftordering {

  def emptySource[X](): Source[X, KillSwitch] = Source.empty
    .viaMat(KillSwitches.single)(Keep.right)

  def fakeSequencerId(name: String): SequencerId =
    SequencerId(UniqueIdentifier.tryCreate("ns", s"fake_$name"))
}
