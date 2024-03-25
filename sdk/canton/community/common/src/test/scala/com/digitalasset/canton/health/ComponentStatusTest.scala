// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.error.TestGroup
import com.digitalasset.canton.health.ComponentHealthState.UnhealthyState
import com.digitalasset.canton.health.ComponentStatus
import com.digitalasset.canton.logging.pretty.PrettyUtil

class ComponentStatusTest extends BaseTestWordSpec with PrettyUtil {
  "ComponentHealthState" should {
    "pretty print Ok" in {
      ComponentStatus(
        "component",
        ComponentHealthState.Ok(),
      ).toString shouldBe "component : Ok()"
    }

    "pretty print Ok w/ details" in {
      ComponentStatus(
        "component",
        ComponentHealthState.Ok(Some("good stuff")),
      ).toString shouldBe "component : Ok(good stuff)"
    }

    "pretty print Failed" in {
      ComponentStatus(
        "component",
        ComponentHealthState.failed("broken"),
      ).toString shouldBe "component : Failed(broken)"
    }

    "pretty print Failed w/ error" in {
      loggerFactory.suppressErrors(
        ComponentStatus(
          "component",
          ComponentHealthState.Failed(
            UnhealthyState(Some("broken"), Some(TestGroup.NestedGroup.MyCode.MyError("bad")))
          ),
        ).toString shouldBe s"component : Failed(broken, error = NESTED_CODE(2,0): this is my error)"
      )
    }

    "pretty print Degraded" in {
      ComponentStatus(
        "component",
        ComponentHealthState.degraded("broken"),
      ).toString shouldBe "component : Degraded(broken)"
    }

    "pretty print Degraded w/ error" in {
      loggerFactory.suppressErrors(
        ComponentStatus(
          "component",
          ComponentHealthState.Degraded(
            UnhealthyState(Some("broken"), Some(TestGroup.NestedGroup.MyCode.MyError("bad")))
          ),
        ).toString shouldBe s"component : Degraded(broken, error = NESTED_CODE(2,0): this is my error)"
      )
    }
  }
}
