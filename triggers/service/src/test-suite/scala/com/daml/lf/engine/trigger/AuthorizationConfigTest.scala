// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import org.scalatest.Succeeded
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException

import scala.util.{Failure, Success}

class AuthorizationConfigTest extends AsyncWordSpec with Matchers {

  import Cli._

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
      scala.util.Try(ConfigSource.string(c).loadOrThrow[AuthorizationConfig]) match {
        case Success(_) =>
          fail("Should fail on supplying both auth-common and auth-internal/auth-external uris")
        case Failure(ex) => ex shouldBe a[ConfigReaderException[_]]
      }
    }
    Succeeded
  }

  "should error on specifying only authInternalUri and no authExternalUri" in {
    scala.util.Try {
      ConfigSource
        .string("""
          |{
          |  auth-internal-uri = "https://oauth2/internal-uri"
          |}
          |""".stripMargin)
        .loadOrThrow[AuthorizationConfig]
    } match {
      case Success(_) => fail("Should fail on only auth-internal uris")
      case Failure(ex) => ex shouldBe a[ConfigReaderException[_]]
    }
  }

}
