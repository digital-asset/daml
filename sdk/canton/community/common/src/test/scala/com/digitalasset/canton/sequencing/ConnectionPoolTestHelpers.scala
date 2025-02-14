// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.topology.{Namespace, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.{BaseTest, HasExecutionContext}

trait ConnectionPoolTestHelpers { this: BaseTest & HasExecutionContext =>
  protected def testSynchronizerId(index: Int): SynchronizerId =
    SynchronizerId.tryFromString(s"test-synchronizer-$index::namespace")
  protected def testSequencerId(index: Int): SequencerId =
    SequencerId.tryCreate(s"test-sequencer-$index", Namespace(Fingerprint.tryCreate("namespace")))

  protected def mkDummyConnectionConfig(
      index: Int,
      endpointIndexO: Option[Int] = None,
  ): ConnectionXConfig = {
    val endpoint = Endpoint(s"does-not-exist-${endpointIndexO.getOrElse(index)}", Port.tryCreate(0))
    ConnectionXConfig(
      name = s"test-$index",
      endpoint = endpoint,
      transportSecurity = false,
      customTrustCertificates = None,
      tracePropagation = TracingConfig.Propagation.Disabled,
    )

  }
}
