// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.generator

import better.files.File
import com.daml.test.evidence.generator.TestEntry.{ReliabilityTestEntry, SecurityTestEntry}
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.daml.test.evidence.tag.Security.{Attack, HappyCase, HappyOrAttack, SecurityTest}
import com.github.tototoshi.csv.CSVWriter

object TestEntryCsvEncoder {

  sealed trait TestEntryCsv extends Product with Serializable {
    def header: Seq[String] = productElementNames.toSeq

    def values: Seq[Any] = productIterator.toSeq
  }

  def write[A <: TestEntryCsv](file: File, values: Seq[A]): Unit = {
    values.headOption.foreach { first =>
      CSVWriter.open(file.toJava).writeAll(first.header +: values.map(_.values))
    }
  }

  /** A flattened representation of a security test entry for CSV exporting. */
  final case class SecurityTestEntryCsv(
      testSuiteName: String,
      testDescription: String,
      property: String,
      asset: String,
      attacker: String,
      threat: String,
      mitigation: String,
      happyCase: String,
      unimplemented: Boolean,
      ignored: Boolean,
      testPath: String,
  ) extends TestEntryCsv

  object SecurityTestEntryCsv {

    def apply(securityTestEntry: SecurityTestEntry): SecurityTestEntryCsv = {
      def getAttackerThreatMitigation(scenario: Option[HappyOrAttack]): (String, String, String) =
        scenario match {
          case Some(Right(Attack(actor, threat, mitigation))) => (actor, threat, mitigation)
          case _ => ("", "", "")
        }

      def happyCase(scenario: Option[HappyOrAttack]): String = scenario match {
        case Some(Left(HappyCase(description))) => description
        case _ => ""
      }

      securityTestEntry match {
        case SecurityTestEntry(
              suiteName,
              description,
              SecurityTest(property, asset, scenario, unimplemented, file @ _, line @ _),
              ignored,
              suite @ _,
            ) =>
          val (attacker, threat, mitigation) = getAttackerThreatMitigation(scenario)
          SecurityTestEntryCsv(
            testSuiteName = suiteName,
            testDescription = description,
            property = property.productPrefix,
            asset = asset,
            attacker = attacker,
            threat = threat,
            mitigation = mitigation,
            happyCase = happyCase(scenario),
            unimplemented = unimplemented,
            ignored = ignored,
            testPath = s"$file:$line",
          )
      }
    }
  }

  /** A flattened representation of a reliability test entry for CSV exporting. */
  final case class ReliabilityTestEntryCsv(
      testSuiteName: String,
      testDescription: String,
      component: String,
      componentSetting: String,
      failedDependency: String,
      failure: String,
      remediation: String,
      remediator: String,
      outcome: String,
      ignored: Boolean,
      testPath: String,
  ) extends TestEntryCsv

  object ReliabilityTestEntryCsv {

    def apply(reliabilityTestEntry: ReliabilityTestEntry): ReliabilityTestEntryCsv = {

      reliabilityTestEntry match {
        case ReliabilityTestEntry(
              suiteName,
              description,
              ReliabilityTest(
                Component(name, setting),
                AdverseScenario(dependency, details),
                Remediation(remediator, action),
                outcome,
                file @ _,
                line @ _,
              ),
              ignored,
              suite @ _,
            ) =>
          ReliabilityTestEntryCsv(
            testSuiteName = suiteName,
            testDescription = description,
            component = name,
            componentSetting = setting,
            failedDependency = dependency,
            failure = details,
            remediator = remediator,
            remediation = action,
            outcome = outcome,
            ignored = ignored,
            testPath = s"$file:$line",
          )
      }
    }
  }
}
