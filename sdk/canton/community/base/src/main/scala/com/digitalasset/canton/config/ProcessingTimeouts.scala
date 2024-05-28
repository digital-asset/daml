// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.google.common.annotations.VisibleForTesting

import scala.concurrent.duration.*

/** Configuration for internal await timeouts
  *
  * @param unbounded timeout on how long "unbounded" operations can run. should be infinite in theory.
  * @param io timeout for disk based operations
  * @param default default finite processing timeout
  * @param network timeout for things related to networking
  * @param shutdownProcessing timeout used for shutdown of some processing where we'd like to keep the result (long)
  * @param shutdownNetwork timeout used for shutdown where we interact with some remote system
  * @param shutdownShort everything else shutdown releated (default)
  * @param closing our closing time (which should be strictly larger than any of the shutdown values)
  * @param verifyActive how long should we wait for the domain to tell us whether we are active or not
  * @param inspection timeout for the storage inspection commands (can run a long long time)
  * @param storageMaxRetryInterval max retry interval for storage
  * @param activeInit how long a passive replica should wait for the initialization by the active replica
  * @param slowFutureWarn when using future supervision, when should we start to warn about a slow future
  * @param activeInitRetryDelay delay between attempts while waiting for initialization of the active replica
  * @param sequencerInfo how long are we going to try to get the sequencer connection information. setting this high means that
  *                      connect calls will take quite a while if one of the sequencers is offline.
  * @param topologyChangeWarnDelay maximum delay between the timestamp of the topology snapshot used during
  *                                submission and the sequencing timestamp, after which we log inconsistency
  *                                errors as warnings
  */
final case class ProcessingTimeout(
    unbounded: NonNegativeDuration = DefaultProcessingTimeouts.unbounded,
    io: NonNegativeDuration = DefaultProcessingTimeouts.io,
    default: NonNegativeDuration = DefaultProcessingTimeouts.default,
    network: NonNegativeDuration = DefaultProcessingTimeouts.network,
    shutdownProcessing: NonNegativeDuration = DefaultProcessingTimeouts.shutdownProcessing,
    shutdownNetwork: NonNegativeDuration = DefaultProcessingTimeouts.shutdownNetwork,
    shutdownShort: NonNegativeDuration = DefaultProcessingTimeouts.shutdownShort,
    closing: NonNegativeDuration = DefaultProcessingTimeouts.closing,
    inspection: NonNegativeDuration = DefaultProcessingTimeouts.inspection,
    storageMaxRetryInterval: NonNegativeDuration = DefaultProcessingTimeouts.maxRetryInterval,
    verifyActive: NonNegativeDuration = DefaultProcessingTimeouts.verifyActive,
    activeInit: NonNegativeDuration = DefaultProcessingTimeouts.activeInit,
    slowFutureWarn: NonNegativeDuration = DefaultProcessingTimeouts.slowFutureWarn,
    activeInitRetryDelay: NonNegativeDuration = DefaultProcessingTimeouts.activeInitRetryDelay,
    sequencerInfo: NonNegativeDuration = DefaultProcessingTimeouts.sequencerInfo,
    topologyChangeWarnDelay: NonNegativeDuration = DefaultProcessingTimeouts.topologyChangeWarnDelay,
)

/** Reasonable default timeouts */
object DefaultProcessingTimeouts {
  val unbounded: NonNegativeDuration = NonNegativeDuration.tryFromDuration(Duration.Inf)

  /** Allow unbounded processing for io operations. This is because we retry forever upon db outages.
    */
  val io: NonNegativeDuration = unbounded

  val default: NonNegativeDuration = NonNegativeDuration.tryFromDuration(1.minute)

  val network: NonNegativeDuration = NonNegativeDuration.tryFromDuration(2.minute)

  val shutdownNetwork: NonNegativeDuration = NonNegativeDuration.tryFromDuration(9.seconds)

  val shutdownProcessing: NonNegativeDuration = NonNegativeDuration.tryFromDuration(60.seconds)

  val shutdownShort: NonNegativeDuration = NonNegativeDuration.tryFromDuration(3.seconds)

  val closing: NonNegativeDuration = NonNegativeDuration.tryFromDuration(10.seconds)

  val inspection: NonNegativeDuration = NonNegativeDuration.tryFromDuration(Duration.Inf)

  val maxRetryInterval: NonNegativeDuration = NonNegativeDuration.tryFromDuration(10.seconds)

  val verifyActive: NonNegativeDuration = NonNegativeDuration.tryFromDuration(60.seconds)

  val activeInit: NonNegativeDuration = NonNegativeDuration.tryFromDuration(1.minute)

  val activeInitRetryDelay: NonNegativeDuration = NonNegativeDuration.tryFromDuration(50.millis)

  val slowFutureWarn: NonNegativeDuration = NonNegativeDuration.tryFromDuration(5.seconds)

  val sequencerInfo: NonNegativeDuration = NonNegativeDuration.tryFromDuration(30.seconds)

  val topologyChangeWarnDelay: NonNegativeDuration = NonNegativeDuration.tryFromDuration(2.minutes)

  @VisibleForTesting
  lazy val testing: ProcessingTimeout = ProcessingTimeout()

}
