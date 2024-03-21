// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class RouteSetupTest extends AnyFreeSpec with Matchers {
  "Forwarded" - {
    import RouteSetup.Forwarded
    "can 'parse' sample" in {
      Forwarded("for=192.168.0.1;proto=http;by=192.168.0.42").proto should ===(Some("http"))
    }

    "can 'parse' quoted sample" in {
      Forwarded("for=192.168.0.1;proto = \"https\" ;by=192.168.0.42").proto should ===(
        Some("https")
      )
    }
  }
}
