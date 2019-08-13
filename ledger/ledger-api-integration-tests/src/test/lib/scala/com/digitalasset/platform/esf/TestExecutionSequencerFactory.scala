// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.esf

import akka.actor.ActorSystem
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestExecutionSequencerFactory extends BeforeAndAfterAll { self: Suite =>

  private lazy val executionSequencerFactory: ExecutionSequencerFactory = {
    if (system == null)
      throw new IllegalStateException(
        "ActorSystem was not initialized for TestExecutionSequncerFactory's beforeAll method.")
    new AkkaExecutionSequencerPool("esf-" + this.getClass.getSimpleName)(system)
  }

  protected def system: ActorSystem

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val _ = executionSequencerFactory
  }
  override protected def afterAll(): Unit = {
    executionSequencerFactory.close()
    super.afterAll()
  }

  implicit protected def esf: ExecutionSequencerFactory = executionSequencerFactory
}
