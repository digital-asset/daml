// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.admin.sequencer.v30
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration

/** Configures the various delays used by the sequencer connection pool.
  *
  * @param minRestartDelay
  *   Minimum duration after which a failed sequencer connection is restarted.
  * @param maxRestartDelay
  *   Maximum duration after which a failed sequencer connection is restarted.
  * @param warnValidationDelay
  *   The duration after which a warning is issued if a started connection still fails validation.
  * @param subscriptionRequestDelay
  *   Delay between the attempts to obtain new sequencer connections for the sequencer subscription
  *   pool, when the current number of subscriptions is below `trustThreshold` + `livenessMargin`.
  */
final case class SequencerConnectionPoolDelays(
    minRestartDelay: NonNegativeFiniteDuration,
    maxRestartDelay: NonNegativeFiniteDuration,
    warnValidationDelay: NonNegativeFiniteDuration,
    subscriptionRequestDelay: NonNegativeFiniteDuration,
) extends PrettyPrinting {
  private[sequencing] def toProtoV30: v30.SequencerConnectionPoolDelays =
    v30.SequencerConnectionPoolDelays(
      minRestartDelay = Some(minRestartDelay.toProtoPrimitive),
      maxRestartDelay = Some(maxRestartDelay.toProtoPrimitive),
      subscriptionRequestDelay = Some(subscriptionRequestDelay.toProtoPrimitive),
      warnValidationDelay = Some(warnValidationDelay.toProtoPrimitive),
    )

  override protected def pretty: Pretty[SequencerConnectionPoolDelays] = prettyOfClass(
    param("minRestartDelay", _.minRestartDelay),
    param("maxRestartDelay", _.maxRestartDelay),
    param("warnValidationDelay", _.warnValidationDelay),
    param("subscriptionRequestDelay", _.subscriptionRequestDelay),
  )
}

object SequencerConnectionPoolDelays {
  val default: SequencerConnectionPoolDelays = SequencerConnectionPoolDelays(
    minRestartDelay = NonNegativeFiniteDuration.tryOfMillis(10),
    maxRestartDelay = NonNegativeFiniteDuration.tryOfSeconds(10),
    warnValidationDelay = NonNegativeFiniteDuration.tryOfSeconds(20),
    subscriptionRequestDelay = NonNegativeFiniteDuration.tryOfSeconds(1),
  )

  private[sequencing] def fromProtoV30(
      proto: v30.SequencerConnectionPoolDelays
  ): ParsingResult[SequencerConnectionPoolDelays] = {
    val v30.SequencerConnectionPoolDelays(
      minRestartDelayP,
      maxRestartDelayP,
      subscriptionRequestDelayP,
      warnValidationDelayP,
    ) = proto

    for {
      minRestartDelay <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("min_restart_delay"),
        "min_restart_delay",
        minRestartDelayP,
      )
      maxRestartDelay <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("max_restart_delay"),
        "min_restart_delay",
        maxRestartDelayP,
      )
      subscriptionRequestDelay <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("subscription_request_delay"),
        "subscription_request_delay",
        subscriptionRequestDelayP,
      )
      // data continuity:
      // `warn_validation_delay` was added to the proto afterward, so it does not exist in the DB for older nodes
      warnValidationDelay <- warnValidationDelayP
        .map(NonNegativeFiniteDuration.fromProtoPrimitive("warn_validation_delay"))
        .getOrElse(Right(default.warnValidationDelay))
    } yield SequencerConnectionPoolDelays(
      minRestartDelay = minRestartDelay,
      maxRestartDelay = maxRestartDelay,
      warnValidationDelay = warnValidationDelay,
      subscriptionRequestDelay = subscriptionRequestDelay,
    )
  }
}
