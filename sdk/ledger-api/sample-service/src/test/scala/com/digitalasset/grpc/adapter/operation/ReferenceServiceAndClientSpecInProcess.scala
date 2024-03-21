// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import com.daml.grpc.adapter.client.ReferenceClientCompatibilityCheck
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ReferenceServiceAndClientSpecInProcess
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with ReferenceClientCompatibilityCheck
    with ReferenceServiceFixture {

  "Reference service" when {

    "testing with reference client" should {
      behave like referenceClientCompatible(clientStub)
    }
  }

  override def socketAddress = None
}
