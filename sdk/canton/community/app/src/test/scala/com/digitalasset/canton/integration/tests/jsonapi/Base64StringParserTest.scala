// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}

class Base64StringParserTest
    extends HttpTestFuns
    with TestCommands
    with HttpServiceUserFixture.UserToken
    with ErrorsAssertions {

  "json api should reject invalid base64 string in body payload" in httpTestFixture { fixture =>
    fixture
      .postJsonStringRequestEncoded(
        fixture.uri withPath Uri.Path("/v2/parties/external/generate-topology"),
        generateInvalidJson(),
        headersWithAdminAuth,
      )
      .map { case (status, result) =>
        status should be(StatusCodes.BadRequest)
        import com.digitalasset.canton.http.json.v2.JsSchema.BYTE_STRING_PARSE_ERROR_TEMPLATE
        val expectedMessage = BYTE_STRING_PARSE_ERROR_TEMPLATE.format("not-base64")
        result should include(expectedMessage)
      }
  }

  def generateInvalidJson(): String =
    """{
      |  "synchronizer": "%s",
      |  "partyHint": "Alice",
      |  "publicKey": {
      |     "format" : "CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO",
      |     "keyData": "%s",
      |     "keySpec" : "SIGNING_KEY_SPEC_EC_CURVE25519"
      |  }
      |}
      |""".stripMargin.formatted(
      "sequencer1",
      "not-base64",
    )

}
