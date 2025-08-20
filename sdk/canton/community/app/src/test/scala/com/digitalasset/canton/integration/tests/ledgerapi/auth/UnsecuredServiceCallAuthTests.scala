// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

trait UnsecuredServiceCallAuthTests extends ServiceCallAuthTests {
  serviceCallName should {

    "allow unauthenticated calls" in { implicit env =>
      import env.*

      expectSuccess(serviceCall(noToken))
    }
  }
}
