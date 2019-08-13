// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter.operation

import com.digitalasset.grpc.adapter.client.ReferenceClientCompatibilityCheck
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class ReferenceServiceAndClientSpecInProcess
    extends WordSpec
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
