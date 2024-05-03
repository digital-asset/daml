// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.metrics.MetricsFactoryType
import com.digitalasset.canton.metrics.MetricsFactoryType.External

/** Used to set parameters for testing when these don't need to be exposed in a config file.
  *
  * @param testSequencerClientFor: members that should use a
  * [[com.digitalasset.canton.sequencing.client.DelayedSequencerClient]] for testing
  * @param initializeGlobalOpenTelemetry Determines whether the OpenTelemetry instance we build is set as the global OpenTelemetry instance. This is set to false during tests to
  *                                      prevent failures as the global OpenTelemetry instance can be initialized just once.
  * @param doNotUseCommitmentCachingFor A participant whose participant.uid.identifier that matches one of these strings will be excluded from the commitment caching.
  * @param reinterpretationTestHookFor Hooks allowing participants to perform actions during reinterpretation interruptions.
  *                                    The argument to pass is a function that takes a string as an argument and returns
  *                                    the hook for the corresponding member whose identifier matches the string.
  *                                    The hook itself is a function that takes no argument and returns `Unit`.
  *                                    The default value provides hooks that do nothing for all members.
  *
  *                                    Example:
  *                                    {{{
  *                                    def myHooks(identifier: String): () => Unit =
  *                                      identifier match {
  *                                        case "participant1" => () => println(s"do something special for `participant1`")
  *                                        case _ => () => println(s"no action")
  *                                      }
  *                                    }}}
  *
  *                                    See also the example in `EngineComputationAbortIntegrationTest`.
  */
final case class TestingConfigInternal(
    testSequencerClientFor: Set[TestSequencerClientFor] = Set.empty,
    metricsFactoryType: MetricsFactoryType = External,
    initializeGlobalOpenTelemetry: Boolean = true,
    doNotUseCommitmentCachingFor: Set[String] = Set.empty,
    reinterpretationTestHookFor: String => () => Unit = _ => () => (),
)

/** @param environmentId ID used to disambiguate tests running in parallel
  * @param memberName The name of the member that should use a delayed sequencer client
  * @param domainName The name of the domain for which the member should use a delayed sequencer client
  */
final case class TestSequencerClientFor(
    environmentId: String,
    memberName: String,
    domainName: String,
)
