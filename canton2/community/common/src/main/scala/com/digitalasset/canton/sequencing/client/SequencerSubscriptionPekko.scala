// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.health.HealthComponent
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.util.PekkoUtil.WithKillSwitch
import org.apache.pekko.Done
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Wrapper for an Pekko source delivering the stream of sequenced events.
  * The [[org.apache.pekko.stream.KillSwitch]] can be used to terminate the stream.
  * The materialized [[scala.concurrent.Future]] completes
  * after the internal processing in the source has finished
  * after having been closed through the [[org.apache.pekko.stream.KillSwitch]].
  */
final case class SequencerSubscriptionPekko[+E](
    source: Source[WithKillSwitch[Either[E, OrdinarySerializedEvent]], (KillSwitch, Future[Done])],
    health: HealthComponent,
)
