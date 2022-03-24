// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.scalatest

import com.daml.test.evidence.tag.Operability.OperabilityTest

trait OperabilityTestHelpers extends AccessTestScenario {

  def operabilityTest(component: String)(dependency: String)(setting: String)(
      cause: String
  )(remediation: String): ResultOfTaggedAsInvocationOnString = {
    import ScalaTestSupport.Implicits._
    new WordSpecStringWrapper(remediation)
      .taggedAs(
        OperabilityTest(
          component = component,
          dependency = dependency,
          setting = setting,
          cause = cause,
          remediation = remediation,
        )
      )
  }

}
