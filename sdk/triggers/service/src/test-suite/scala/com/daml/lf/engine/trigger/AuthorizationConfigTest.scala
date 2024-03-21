// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import org.scalatest.Succeeded
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import pureconfig.ConfigSource
import pureconfig.error.{ConfigReaderFailures, ConvertFailure}
import org.scalatest.Assertion

class AuthorizationConfigTest extends AsyncWordSpec with Matchers {

  private def validateFailure(ex: ConfigReaderFailures): Assertion = {
    ex.head match {
      case ConvertFailure(reason, _, _) =>
        reason shouldBe a[AuthorizationConfig.AuthConfigFailure.type]
        reason.description shouldBe AuthorizationConfig.AuthConfigFailure.description
      case _ => fail("Unexpected failure type expected `AuthConfigFailure`")
    }
  }

  // TEST_EVIDENCE: Authorization: error on specifying both authCommonUri and authInternalUri/authExternalUri for the trigger service
  "should error on specifying both authCommonUri and authInternalUri/authExternalUri" in {
    val invalidConfigs = List(
      """
        |{
        |  auth-common-uri = "https://oauth2/common-uri"
        |  auth-internal-uri = "https://oauth2/internal-uri"
        |}
        |""".stripMargin,
      """
        |{
        |  auth-common-uri = "https://oauth2/common-uri"
        |  auth-external-uri = "https://oauth2/external-uri"
        |}
        |""".stripMargin,
    )

    invalidConfigs.foreach { c =>
      ConfigSource.string(c).load[AuthorizationConfig] match {
        case Right(_) =>
          fail("Should fail on supplying both auth-common and auth-internal/auth-external uris")
        case Left(ex) =>
          validateFailure(ex)
      }
    }
    Succeeded
  }

  // TEST_EVIDENCE: Authorization: error on specifying only authInternalUri and no authExternalUri for the trigger service
  "should error on specifying only authInternalUri and no authExternalUri" in {
    ConfigSource
      .string("""
          |{
          |  auth-internal-uri = "https://oauth2/internal-uri"
          |}
          |""".stripMargin)
      .load[AuthorizationConfig] match {
      case Right(_) => fail("Should fail on only auth-internal uris")
      case Left(ex) =>
        validateFailure(ex)
    }
  }

}
