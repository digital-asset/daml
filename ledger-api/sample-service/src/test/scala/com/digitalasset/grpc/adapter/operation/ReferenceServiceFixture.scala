// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter.operation

import com.digitalasset.grpc.sampleservice.implementations.ReferenceImplementation
import com.digitalasset.ledger.api.testing.utils.{
  GrpcServiceFixture,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.platform.hello.HelloServiceGrpc
import com.digitalasset.platform.hello.HelloServiceGrpc.HelloServiceStub

import java.util.concurrent.TimeUnit

trait ReferenceServiceFixture
    extends GrpcServiceFixture[HelloServiceStub]
    with SuiteResourceManagementAroundAll {

  protected lazy val channel = suiteResource.value.channel()
  protected lazy val clientStub = HelloServiceGrpc.stub(channel)

  override protected def afterAll(): Unit = {
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
    super.afterAll()
  }

  override protected def services =
    List(new ReferenceImplementation())

}
