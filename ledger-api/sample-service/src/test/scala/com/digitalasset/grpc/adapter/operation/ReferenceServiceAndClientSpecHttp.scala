// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import com.daml.grpc.adapter.client.ReferenceClientCompatibilityCheck
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import java.net.InetSocketAddress

class ReferenceServiceAndClientHttpSpec
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
  override def socketAddress = Some(new InetSocketAddress("127.0.0.1", 0))
}
