// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.metrics.MetricsFactoryType
import com.digitalasset.canton.metrics.MetricsFactoryType.External
import com.digitalasset.canton.topology.Identifier

/** Used to set parameters for testing when these don't need to be exposed in a config file.
  *
  * @param testSequencerClientFor: members that should use a
  * [[com.digitalasset.canton.sequencing.client.DelayedSequencerClient]] for testing
  * @param initializeGlobalOpenTelemetry Determines whether the OpenTelemetry instance we build is set as the global OpenTelemetry instance. This is set to false during tests to
  *                                      prevent failures as the global OpenTelemetry instance can be initialized just once.
  */
final case class TestingConfigInternal(
    testSequencerClientFor: Set[TestSequencerClientFor] = Set.empty,
    metricsFactoryType: MetricsFactoryType = External,
    initializeGlobalOpenTelemetry: Boolean = true,
    doNotUseCommitmentCachingFor: Set[Identifier] = Set.empty,
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
