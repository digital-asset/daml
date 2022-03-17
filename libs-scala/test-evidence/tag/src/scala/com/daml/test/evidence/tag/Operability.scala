// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.tag

object Operability {

  trait OperabilityTestSuite

  object OperabilityTestSuite {}

  final case class OperabilityTest(
      component: String,
      dependency: String,
      setting: String,
      cause: String,
      remediation: String,
  ) extends EvidenceTag

}
