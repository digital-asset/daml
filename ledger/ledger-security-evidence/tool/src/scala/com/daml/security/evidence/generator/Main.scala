// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.security.evidence.generator

import better.files.File
import cats.syntax.functorFilter._
import com.daml.ledger.api.testtool.suites
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.security.evidence.generator.TestEntry.SecurityTestEntry
import com.daml.security.evidence.generator.TestEntry.ReliabilityTestEntry
import io.circe.syntax._
import com.daml.security.evidence.tag.Reliability.{ReliabilityTest, ReliabilityTestSuite}
import com.daml.security.evidence.tag.Security.{SecurityTest, SecurityTestSuite}
import com.daml.security.evidence.tag.EvidenceTag
import io.circe.generic.auto._
import org.scalatest.Suite
import com.daml.security.evidence.scalatest.JsonCodec.SecurityJson._
import com.daml.security.evidence.scalatest.JsonCodec.ReliabilityJson._
import org.scalatest.daml.ScalaTestAdapter
import scala.reflect.ClassTag

object Main {

  private def testEntries[TT: ClassTag, TS: ClassTag, TE](
      suites: List[LedgerTestSuite],
      testEntry: (String, String, TT, Boolean, Option[TS]) => TE,
  ): List[TE] = {
    suites.flatMap { suite =>
      val testSuite = suite match {
        case testSuite: TS => Some(testSuite)
        case _ => None
      }

      val tags: Seq[(String, List[EvidenceTag])] = suite.tests.map { test =>
        test.name -> test.tags
      }

      tags.mapFilter { case (testName, testTags) =>
        testTags.collectFirst { case testTag: TT =>
          testEntry(suite.name, testName, testTag, false, testSuite)
        }
      }
    }
  }

  private def loadIntelliJClasspath(): Option[String] =
    Some(System.getProperty("java.class.path")).filter(!_.contains("sbt-launch.jar"))

  def main(args: Array[String]): Unit = {
    val ledgerApiTests = (suites.v1_8.default(0L) ++ suites.v1_8.optional()).toList

    val cp: Seq[String] = loadIntelliJClasspath()
      .getOrElse(
        sys.error("Currently I only support this to be run in Intellij")
      )
      .split(":")
      .toSeq
    println("cp: " + cp.mkString(","))
    val runpathList = cp.toList

    val testSuites: List[Suite] = ScalaTestAdapter.loadTestSuites(runpathList)

    println()

    println("Writing security tests inventory..")
    val securityTestsFilePath = File("security-tests.json")
      .write(
        ScalaTestGeneratorSupport.testEntries[SecurityTest, SecurityTestSuite, SecurityTestEntry](
          testSuites,
          SecurityTestEntry,
        ).asJson.spaces2
      )
      .path
      .toAbsolutePath
      .toString
    println(s"Wrote to $securityTestsFilePath")

    println("Writing reliability tests inventory..")
    val reliabilityTestsFilePath = File("reliability-tests.json")
      .write(
        ScalaTestGeneratorSupport.testEntries[ReliabilityTest, ReliabilityTestSuite, ReliabilityTestEntry](
          testSuites,
          ReliabilityTestEntry,
        ).asJson.spaces2
      )
      .path
      .toAbsolutePath
      .toString
    println(s"Wrote to $reliabilityTestsFilePath")

    println("Writing Ledger Api tests inventory..")
    val ledgerApiTestsFilePath = File("ledger-api-tests.json")
      .write(
        testEntries[SecurityTest, SecurityTestSuite, SecurityTestEntry](
          ledgerApiTests,
          SecurityTestEntry,
        ).asJson.spaces2
      )
      .path
      .toAbsolutePath
      .toString
    println(s"Wrote to $ledgerApiTestsFilePath")

    sys.exit()
  }
}
