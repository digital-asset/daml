// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.tag

import better.files.File

object Reliability {

  // TODO test evidencing: Capture test-suite level data for reliability tests
  trait ReliabilityTestSuite

  /** A reliability relevant test.
    *
    * Generally, for a given failure or degradation of a component, the next dependent of the component which can remediate the failure
    * should be the component that should be tested/annotated to remain available. If the failing component 'itself'
    * can remediate the failure (e.g. when the active mediator crashes, the other passive replicated mediators will
    * remediate by becoming active), then the next dependent should be annotated as the component to remain available.
    *
    * For example, if the sequencer-database connection is blocked, then the component that should remain available
    * is the sequencer as next dependent. It should, e.g., not be the participant connected to the sequencer (which also wouldn't be able to
    * process transactions over that sequencer anymore) because the participant is not the next dependent.
    *
    * @param component         the component that should remain available, continue processing or recover
    * @param dependencyFailure the failure scenario of a dependency and its remediation
    * @param remediation       how the system remediates the adverse scenario
    * @param outcome           in what way the component is still available, can continue operating or recovers
    * @param file              The filename that contains the test
    * @param line              The line number of the test
    */
  final case class ReliabilityTest(
      component: Component,
      dependencyFailure: AdverseScenario,
      remediation: Remediation,
      outcome: String,
      file: String,
      line: Int,
  ) extends EvidenceTag

  object ReliabilityTest {

    def apply(
        component: Component,
        dependencyFailure: AdverseScenario,
        remediation: Remediation,
        outcome: String,
    )(implicit
        lineNo: sourcecode.Line,
        fileName: sourcecode.File,
    ): ReliabilityTest = {
      val relPath = File.currentWorkingDirectory.relativize(File(fileName.value))
      ReliabilityTest(
        component = component,
        dependencyFailure = dependencyFailure,
        remediation = remediation,
        outcome = outcome,
        file = relPath.toString,
        line = lineNo.value,
      )
    }
  }

  /** @param remediator the component carrying out the remediation
    * @param action      what steps are taken for the remediation
    */
  case class Remediation(remediator: String, action: String)

  /** @param name   name of the component that must remain available, continue processing or recover
    * @param setting whether the component is replicated, embedded etc.
    */
  case class Component(name: String, setting: String)

  /** Description of a scenario where the dependency of a component is adversely impacted.
    * This can be any sort of (usually network) failure or degradation that could adversely affect the reliability
    * of the component and requires logic within Canton (e.g. retries) to stay reliable. It should not be security-
    * relevant scenarios.
    * This should also not be a scenario that essentially only tests the underlying network stack (e.g. slow bandwidth)
    * and doesn't test anything Canton-specific (i.e. where we don't do a specific remediation except correctly handling
    * the possible errors of the underlying network stack).
    *
    * @param dependency what dependency is affected
    * @param details    how the dependency is adversely affected
    */
  case class AdverseScenario(dependency: String, details: String)
}
