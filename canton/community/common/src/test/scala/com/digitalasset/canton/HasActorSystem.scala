// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import org.apache.pekko.actor.ActorSystem
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.util.PekkoUtil
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Mixin to provide an implicit [[pekko.actor.ActorSystem]] to a test suite */
trait HasActorSystem extends BeforeAndAfterAll {
  this: Suite with HasExecutionContext with NamedLogging =>

  protected implicit lazy val actorSystem: ActorSystem =
    PekkoUtil.createActorSystem(getClass.getSimpleName)

  protected def timeouts: ProcessingTimeout

  override def afterAll(): Unit =
    try Lifecycle.close(Lifecycle.toCloseableActorSystem(actorSystem, logger, timeouts))(logger)
    finally super.afterAll()
}
