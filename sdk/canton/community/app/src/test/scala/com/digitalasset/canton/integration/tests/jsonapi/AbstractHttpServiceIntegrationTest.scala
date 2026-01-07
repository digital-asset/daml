// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import org.apache.pekko.http.scaladsl.model.*

trait AbstractHttpServiceIntegrationTestFunsUserToken extends HttpServiceUserFixture.UserToken {
  self: AbstractHttpServiceIntegrationTestFuns =>

  protected def testId: String = this.getClass.getSimpleName

  protected def headersWithUserAuth(
      user: Option[String]
  ) =
    HttpServiceTestFixture.headersWithUserAuth(user)
}

/** Tests that may behave differently depending on
  *
  *   1. whether custom or user tokens are used, ''and''
  *   1. the query store configuration
  */
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class AbstractHttpServiceIntegrationTest extends AbstractHttpServiceIntegrationTestFuns {

  "request non-existent endpoint should return 404 with errors" in httpTestFixture { fixture =>
    val badPath = Uri.Path("/contracts/does-not-exist")
    fixture
      .getRequestWithMinimumAuth_(badPath)
      .map {
        case (StatusCodes.OK, _) =>
          fail(s"Unexpected success accessing an invalid HTTP endpoint: $badPath")
        case (status, _) => status shouldBe StatusCodes.NotFound
      }
  }

}
