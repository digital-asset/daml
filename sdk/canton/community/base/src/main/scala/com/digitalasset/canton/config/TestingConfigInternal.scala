// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.metrics.MetricsFactoryType
import com.digitalasset.canton.metrics.MetricsFactoryType.External

/** Used to set parameters for testing when these don't need to be exposed in a config file.
  *
  * @param testSequencerClientFor:
  *   members that should use a [[com.digitalasset.canton.sequencing.client.DelayedSequencerClient]]
  *   for testing
  * @param supportAdhocMetrics
  *   if true, then creating adhoc metrics is supported (conflicts with histogram redefinitions)
  * @param initializeGlobalOpenTelemetry
  *   Determines whether the OpenTelemetry instance we build is set as the global OpenTelemetry
  *   instance. This is set to false during tests to prevent failures as the global OpenTelemetry
  *   instance can be initialized just once.
  * @param doNotUseCommitmentCachingFor
  *   A participant whose participant.uid.identifier that matches one of these strings will be
  *   excluded from the commitment caching.
  * @param reinterpretationTestHookFor
  *   Hooks allowing participants to perform actions during reinterpretation interruptions. The
  *   argument to pass is a function that takes a string as an argument and returns the hook for the
  *   corresponding member whose identifier matches the string. The hook itself is a function that
  *   takes no argument and returns `Unit`. The default value provides hooks that do nothing for all
  *   members.
  *
  * Example:
  * {{{
  *                                    def myHooks(identifier: String): () => Unit =
  *                                      identifier match {
  *                                        case "participant1" => () => println(s"do something special for `participant1`")
  *                                        case _ => () => println(s"no action")
  *                                      }
  * }}}
  *
  * See also the example in `EngineComputationAbortIntegrationTest`.
  * @param maxCommitmentSendDelayMillis
  *   The maximum delay for sending commitments in milliseconds. If not set, commitment sending is
  *   delayed by a random amount at most the default value.
  * @param sequencerTransportSeed
  *   The seed to be used for choosing threshold number of sequencer transports.
  * @param warnOnAcsCommitmentDegradation
  *   When true, we log a warning when the participant falls behind in producing ACS commitments.
  *   This is the default. A false value lowers the logging level to debug, while keeping the
  *   degradation of the health status. In tests, we should set it to false, because any test with
  *   high load can cause a participant to lag behind in producing commitments, which would produce
  *   an ACS commitment degradation warning and cause a CI test to be reported as failed. Because in
  *   our testing framework any test can run colocated with any other test, any test, load-intensive
  *   or not, can issue the warning. Therefore, this parameter should be set to false in tests,
  *   unless the test expressly checks the behavior of the degradation.
  */
final case class TestingConfigInternal(
    testSequencerClientFor: Set[TestSequencerClientFor] = Set.empty,
    metricsFactoryType: MetricsFactoryType = External,
    supportAdhocMetrics: Boolean = false,
    initializeGlobalOpenTelemetry: Boolean = true,
    doNotUseCommitmentCachingFor: Set[String] = Set.empty,
    reinterpretationTestHookFor: String => () => Unit = _ => () => (),
    maxCommitmentSendDelayMillis: Option[NonNegativeInt] = None,
    sequencerTransportSeed: Option[Long] = None,
    participantsWithoutLapiVerification: Set[String] = Set.empty,
    enableInMemoryTransactionStoreForParticipants: Boolean = false,
    warnOnAcsCommitmentDegradation: Boolean = true,
)

/** @param environmentId
  *   ID used to disambiguate tests running in parallel
  * @param memberName
  *   The name of the member that should use a delayed sequencer client
  * @param synchronizerName
  *   The name of the synchronizer for which the member should use a delayed sequencer client
  */
final case class TestSequencerClientFor(
    environmentId: String,
    memberName: String,
    synchronizerName: String,
)
