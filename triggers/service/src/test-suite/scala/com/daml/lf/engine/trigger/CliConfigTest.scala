// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.platform.services.time.TimeProviderType
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CliConfigTest extends AnyWordSpec with Matchers with OptionValues {
  "parse" should {
    import Cli.parse
    import com.daml.cliopts.Http.defaultAddress
    val baseOpts = Array("--ledger-host", "localhost", "--ledger-port", "9999")

    "read address" in {
      parse(baseOpts, Set()).value.address should ===(defaultAddress)
      parse(baseOpts ++ Seq("--address", "0.0.0.0"), Set()).value.address should ===("0.0.0.0")
    }
    "default to postgresql jdbc driver" in {
      parse(
        baseOpts ++ Seq("--jdbc", "url=url,user=user,password=password"),
        Set("org.postgresql.Driver"),
      ).value.jdbcConfig.value.driver should ===("org.postgresql.Driver")
    }
    "support a custom jdbc driver" in {
      parse(
        baseOpts ++ Seq("--jdbc", "driver=custom,url=url,user=user,password=password"),
        Set("custom"),
      ).value.jdbcConfig.value.driver should ===("custom")
    }
    "fails for unsupported jdbc driver" in {
      parse(
        baseOpts ++ Seq("--jdbc", "driver=custom,url=url,user=user,password=password"),
        Set("notcustom"),
      ) should ===(None)
    }

    // TEST_EVIDENCE: Input Validation: auth and auth-* should not be set together for the trigger service
    "auth and auth-* should not be set together" in {
      parse(baseOpts ++ Seq("--auth", "http://example.com"), Set()) should !==(None)
      parse(
        baseOpts ++ Seq(
          "--auth-internal",
          "http://example.com/1",
          "--auth-external",
          "http://example.com/2",
        ),
        Set(),
      ) should !==(None)
      parse(
        baseOpts ++ Seq("--auth", "http://example.com", "--auth-internal", "http://example.com/1"),
        Set(),
      ) should ===(None)
      parse(
        baseOpts ++ Seq("--auth", "http://example.com", "--auth-external", "http://example.com/1"),
        Set(),
      ) should ===(None)
      parse(baseOpts ++ Seq("--auth-internal", "http://example.com/1"), Set()) should ===(None)
      parse(baseOpts ++ Seq("--auth-external", "http://example.com/1"), Set()) should ===(None)
      parse(
        baseOpts ++ Seq(
          "--auth",
          "http://example.com",
          "--auth-internal",
          "http://example.com/1",
          "--auth-external",
          "http://example.com/2",
        ),
        Set(),
      ) should ===(None)
    }
    "default to wall-clock time" in {
      parse(baseOpts, Set()).value.timeProviderType should ===(TimeProviderType.WallClock)
    }
    "safely accept -w without effects" in {
      parse(baseOpts :+ "-w", Set()).value.timeProviderType should ===(TimeProviderType.WallClock)
    }
    "safely accept --wall-clock-time without effects" in {
      parse(baseOpts :+ "--wall-clock-time", Set()).value.timeProviderType should ===(
        TimeProviderType.WallClock
      )
    }
    "optionally use static time (-s)" in {
      parse(baseOpts :+ "-s", Set()).value.timeProviderType should ===(
        TimeProviderType.Static
      )
    }
    "optionally use static time (--static-time)" in {
      parse(baseOpts :+ "--static-time", Set()).value.timeProviderType should ===(
        TimeProviderType.Static
      )
    }
    "defaults to empty table prefix for backwards compatibility" in {
      parse(
        baseOpts ++ Seq("--jdbc", "driver=custom,url=url,user=user,password=password"),
        Set("custom"),
      ).value.jdbcConfig.value.tablePrefix shouldBe empty
    }
    "optionally use a table prefix to avoid collisions" in {
      parse(
        baseOpts ++ Seq(
          "--jdbc",
          "driver=custom,url=url,user=user,password=password,tablePrefix=trigger_service_",
        ),
        Set("custom"),
      ).value.jdbcConfig.value.tablePrefix shouldBe "trigger_service_"
    }
  }
}
