// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.ledger.api.v2.reassignment.UnassignedEvent
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class JsonDecodingTest extends AnyWordSpecLike with Matchers with EitherValues {
  import com.digitalasset.canton.http.json.v2.JsSchema.JsServicesCommonCodecs.*
  "json decoders" should {
    "decode structure with only required properties" in {
      // TODO (i25144) as of now we do not require any properties - it may change in the future
      val json =
        """
           {
              "reassignmentId": "reId"
           }
        """
      io.circe.parser.decode[UnassignedEvent](json).value.reassignmentId shouldBe "reId"
    }

    "do not fail on extra/unspecified fields" in {
      val json =
        """
           {
           "contractId": "cid",
              "reBBassignmentId": "reId"
           }
        """
      io.circe.parser.decode[UnassignedEvent](json).value.reassignmentId shouldBe ""
    }
  }
}
