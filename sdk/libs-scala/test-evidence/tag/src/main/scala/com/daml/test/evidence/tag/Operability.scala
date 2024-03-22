// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.tag

object Operability {

  /** Operability-relevant information on a test-suite level. */
  trait OperabilityTestSuite

  object OperabilityTestSuite {}

  /** A operability-relevant test
    *
    * @param component the component that should remain available and safe, continue processing or recover
    * @param dependency what dependency is affected
    * @param setting specific setting which test is being executed under
    * @param cause thing that gives rise to an action, phenomenon, or condition
    * @param remediation how the system remediates the adverse scenario
    */
  final case class OperabilityTest(
      component: String,
      dependency: String,
      setting: String,
      cause: String,
      remediation: String,
  ) extends EvidenceTag

}
