// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.security.evidence.scalatest


trait OperabilityTestHelpers extends AccessTestScenario {

  def operabilityTest(component: String)(dependency: String)(setting: String)(
    cause: String
  )(remediation: String): ResultOfTaggedAsInvocationOnString = {
    import ScalaTestSupport.Implicits._
    new WordSpecStringWrapper(remediation)
      .taggedAs(
        com.daml.security.evidence.tag.Operability.OperabilityTest(
          component = component,
          dependency = dependency,
          setting = setting,
          cause = cause,
          remediation = remediation,
        )
      )
  }

}
