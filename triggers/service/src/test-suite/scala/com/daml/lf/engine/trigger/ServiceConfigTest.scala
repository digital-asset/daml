// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ServiceConfigTest extends AnyWordSpec with Matchers with OptionValues {
  "parse" should {
    import ServiceConfig.parse
    import com.daml.cliopts.Http.defaultAddress
    val baseOpts = Array("--ledger-host", "localhost", "--ledger-port", "9999")

    "read address" in {
      parse(baseOpts).value.address should ===(defaultAddress)
      parse(baseOpts ++ Seq("--address", "0.0.0.0")).value.address should ===("0.0.0.0")
    }
  }
}
