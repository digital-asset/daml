// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.topology.NodeIdentity

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*

trait IdleTimeSupport {

  val lastOperationTime: TrieMap[NodeIdentity, CantonTimestamp] = TrieMap.empty

  def isNodeReady(node: InstanceReference, minIdleTime: FiniteDuration)(implicit
      env: TestConsoleEnvironment
  ): Boolean =
    lastOperationTime
      .getOrElse(node.id, CantonTimestamp.MinValue)
      .plus(minIdleTime.toJava) <= env.environment.clock.now

  def markNodeOperation(node: InstanceReference)(implicit
      env: TestConsoleEnvironment
  ) = lastOperationTime.put(node.id, env.environment.clock.now)

  def whenNodeReadyAndActive(node: InstanceReference, minIdleTime: FiniteDuration)(
      action: => Unit
  )(implicit
      env: TestConsoleEnvironment
  ): Unit =
    if (node.health.active && node.health.initialized() && isNodeReady(node, minIdleTime)) {
      try action
      finally {
        markNodeOperation(node)
      }
    }
}
