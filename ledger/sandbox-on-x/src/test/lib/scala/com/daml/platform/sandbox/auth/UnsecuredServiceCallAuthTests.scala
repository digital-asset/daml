// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

trait UnsecuredServiceCallAuthTests extends ServiceCallAuthTests {
  behavior of serviceCallName

  it should "allow unauthenticated calls" in {
    expectSuccess(serviceCall(noToken))
  }
}
