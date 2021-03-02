// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.auth.client.LedgerCallCredentials
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.platform.sandbox.SandboxRequiringAuthorization
import com.daml.platform.sandbox.services.SandboxFixture
import io.grpc.stub.AbstractStub
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.Assertion

import scala.concurrent.Future

trait UnsecuredServiceCallAuthTests
    extends AsyncFlatSpec
    with SandboxFixture
    with SandboxRequiringAuthorization
    with SuiteResourceManagementAroundAll
    with Matchers {

  def serviceCallName: String

  def serviceCallWithoutToken(): Future[Any]

  protected def expectSuccess[T](f: Future[T]): Future[Assertion] =
    f.map(_ => succeed)

  protected def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(LedgerCallCredentials.authenticatingStub(stub, _))

  behavior of serviceCallName

  it should "allow unauthenticated calls" in {
    expectSuccess(serviceCallWithoutToken())
  }

}
