// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*

trait SuperReaderServiceCallAuthTests extends ReadOnlyServiceCallAuthTests {

  serviceCallName should {
    "deny calls with a party-wildcard filter for a user that can-read-as main actor" taggedAs securityAsset
      .setAttack(
        attackUnauthenticated(threat =
          "Use a party-wildcard filter for a user that does not have can-read-as-any-party rights"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(canReadAsMainActor.copy(eventFormat = eventFormatPartyWildcard))
      )
    }

    "allow calls with party-wildcard filter for a user that can read as any party" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with party-wildcard filter for a can-read-as-any-party user"
      ) in { implicit env =>
      import env.*
      successfulBehavior(
        serviceCall(
          canReadAsAnyParty.copy(eventFormat = eventFormatPartyWildcard)
        )
      )
    }
  }
}
