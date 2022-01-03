// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import com.daml.grpc.sampleservice.implementations.HelloServiceReferenceImplementation
import com.daml.ledger.api.testing.utils.{GrpcServiceFixture, SuiteResourceManagementAroundAll}
import com.daml.platform.hello.HelloServiceGrpc
import com.daml.platform.hello.HelloServiceGrpc.HelloServiceStub

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
    List(new HelloServiceReferenceImplementation())

}
