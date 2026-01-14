// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.v2.admin.party_management_service.GetParticipantIdResponse
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authenticity
import com.digitalasset.canton.http.json.v2.JsPartyManagementCodecs.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.UseTls
import org.apache.pekko.http.scaladsl.model.Uri

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class TlsTest
    extends AbstractHttpServiceIntegrationTestFuns
    with AbstractHttpServiceIntegrationTestFunsUserToken {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  val authenticationSecurity: SecurityTest =
    SecurityTest(property = Authenticity, asset = "HTTP JSON API Service")

  override def useTls = UseTls.Tls

  "JSON API" should {
    "connect normally with tls on" taggedAs authenticationSecurity.setHappyCase(
      "A client request returns OK with enabled TLS"
    ) in withHttpService() { fixture =>
      fixture
        .getRequestWithMinimumAuth[GetParticipantIdResponse](Uri.Path("/v2/parties/participant-id"))
        .map(_.participantId should (not be empty))
    }
  }
}
