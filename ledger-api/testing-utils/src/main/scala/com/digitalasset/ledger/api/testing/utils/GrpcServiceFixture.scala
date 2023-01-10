// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import io.grpc.BindableService
import org.scalatest.Suite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}

import java.net.SocketAddress

trait GrpcServiceFixture[Stub] extends Suite with ScalaFutures with GrpcServerFixture[Stub] {
  self: SuiteResourceManagement =>

  protected def socketAddress: Option[SocketAddress]

  protected def services: List[BindableService with AutoCloseable]

  override lazy val suiteResource: Resource[ServerWithChannelProvider] =
    new GrpcServerResource(() => services, socketAddress)

}

trait GrpcServerFixture[Stub]
    extends Suite
    with ScalaFutures
    with SuiteResource[ServerWithChannelProvider] {
  self: SuiteResourceManagement =>

  protected def ledgerId = "MOCK_SERVER"

  protected def notLedgerId = "THIS_IS_NOT_THE_LEDGER_ID"

  override implicit lazy val patienceConfig: PatienceConfig = PatienceConfig(Span(60L, Seconds))

}
