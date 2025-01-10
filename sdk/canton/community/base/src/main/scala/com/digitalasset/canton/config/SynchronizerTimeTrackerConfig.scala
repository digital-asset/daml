// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.syntax.option.*
import com.digitalasset.canton.admin.time.v30
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** Configuration for the synchronizer time tracker.
  * @param observationLatency Even if the host and synchronizer clocks are perfectly synchronized there will always be some latency
  *                           for an event to be delivered (storage, transmission, processing).
  *                           If the current host time exceeds the next expected timestamp by this observation latency
  *                           then we will request a time proof (unless we have received a recent event within the
  *                           patience duration described below).
  * @param patienceDuration We will only request a time proof if this given duration has elapsed since we last received
  *                         an event (measured using the host clock). This prevents requesting timestamps when we
  *                         are observing events from the synchronizer (particularly if the local node is catching up on
  *                         old activity).
  * @param minObservationDuration We will try to ensure that we receive a time at least once during this duration (measured
  *                               using the host clock). This is practically useful if there is no other activity on
  *                               the synchronizer as the sequencer client will then have an event to acknowledge allowing
  *                               sequenced events to be pruned before this point. We may in the future use this to monitor
  *                               clock skews between the host and domain.
  * @param timeRequest configuration for how we ask for a time proof.
  */
final case class SynchronizerTimeTrackerConfig(
    observationLatency: NonNegativeFiniteDuration =
      SynchronizerTimeTrackerConfig.defaultObservationLatency,
    patienceDuration: NonNegativeFiniteDuration =
      SynchronizerTimeTrackerConfig.defaultPatienceDuration,
    minObservationDuration: NonNegativeFiniteDuration =
      SynchronizerTimeTrackerConfig.defaultMinObservationDuration,
    timeRequest: TimeProofRequestConfig = TimeProofRequestConfig(),
) extends PrettyPrinting {
  def toProtoV30: v30.SynchronizerTimeTrackerConfig = v30.SynchronizerTimeTrackerConfig(
    observationLatency.toProtoPrimitive.some,
    patienceDuration.toProtoPrimitive.some,
    minObservationDuration.toProtoPrimitive.some,
    timeRequest.toProtoV30.some,
  )

  override protected def pretty: Pretty[SynchronizerTimeTrackerConfig] = prettyOfClass(
    paramIfNotDefault(
      "observationLatency",
      _.observationLatency,
      SynchronizerTimeTrackerConfig.defaultObservationLatency,
    ),
    paramIfNotDefault(
      "patienceDuration",
      _.patienceDuration,
      SynchronizerTimeTrackerConfig.defaultPatienceDuration,
    ),
    paramIfNotDefault(
      "minObservationDuration",
      _.minObservationDuration,
      SynchronizerTimeTrackerConfig.defaultMinObservationDuration,
    ),
    paramIfNotDefault("timeRequest", _.timeRequest, TimeProofRequestConfig()),
  )

}

object SynchronizerTimeTrackerConfig {

  private val defaultObservationLatency: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMillis(250)
  private val defaultPatienceDuration: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMillis(500)
  private val defaultMinObservationDuration: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofHours(24)

  def fromProto(
      configP: v30.SynchronizerTimeTrackerConfig
  ): ParsingResult[SynchronizerTimeTrackerConfig] =
    for {
      observationLatency <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("observation_latency"),
        "observation_latency",
        configP.observationLatency,
      )
      patienceDuration <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("patience_duration"),
        "patience_duration",
        configP.patienceDuration,
      )
      minObservationDuration <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("min_observationDuration"),
        "min_observationDuration",
        configP.minObservationDuration,
      )
      timeProofRequestConfig <- ProtoConverter.parseRequired(
        TimeProofRequestConfig.fromProtoV30,
        "time_proof_request",
        configP.timeProofRequest,
      )
    } yield SynchronizerTimeTrackerConfig(
      observationLatency,
      patienceDuration,
      minObservationDuration,
      timeProofRequestConfig,
    )
}
