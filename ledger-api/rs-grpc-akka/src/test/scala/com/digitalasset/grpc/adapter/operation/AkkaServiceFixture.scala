// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter.operation

import com.digitalasset.grpc.adapter.{ExecutionSequencerFactory, TestExecutionSequencerFactory}
import com.digitalasset.grpc.adapter.utils.implementations.AkkaImplementation
import com.digitalasset.ledger.api.testing.utils._
import com.digitalasset.platform.hello.HelloServiceGrpc
import com.digitalasset.platform.hello.HelloServiceGrpc.HelloServiceStub
import java.net.SocketAddress
import java.util.concurrent.TimeUnit

trait AkkaServiceFixture
    extends GrpcServerFixture[HelloServiceStub]
    with SuiteResourceManagementAroundAll {

  protected lazy val channel = suiteResource.value.channel()
  protected lazy val clientStub = HelloServiceGrpc.stub(channel)

  override protected def afterAll(): Unit = {
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
    super.afterAll()
  }
  protected def socketAddress: Option[SocketAddress]

  override protected def suiteResource: Resource[ServerWithChannelProvider] = resources.map(_._2)

  lazy val resources = AkkaServiceFixture.getResource(socketAddress)

  protected def service: AkkaImplementation =
    resources.getRunningServices.head.asInstanceOf[AkkaImplementation]

}

object AkkaServiceFixture {

  implicit private val esf: ExecutionSequencerFactory = TestExecutionSequencerFactory.instance

  def getResource(address: Option[SocketAddress]): AkkaStreamGrpcServerResource = {
    AkkaStreamGrpcServerResource(implicit m => List(new AkkaImplementation()), "server", address)
  }
}
