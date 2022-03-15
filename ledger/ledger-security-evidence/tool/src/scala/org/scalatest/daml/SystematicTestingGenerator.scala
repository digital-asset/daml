// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.scalatest.daml

import better.files.File
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.traverse._
import cats.syntax.functorFilter._
import com.daml.ledger.api.testtool.suites
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.security.evidence.generator.TestEntry.SecurityTestEntry
import com.daml.security.evidence.generator.TestEntry.ReliabilityTestEntry
import io.circe.parser.decode
import io.circe.syntax._
import com.daml.security.evidence.tag.Reliability.{ReliabilityTest, ReliabilityTestSuite}
import com.daml.security.evidence.tag.Security.{SecurityTest, SecurityTestSuite}
import com.daml.security.evidence.tag.EvidenceTag
import io.circe.generic.auto._
import org.scalatest.Suite
import org.scalatest.tools.{DiscoverySuite, Runner, SuiteDiscoveryHelper}
import com.daml.security.evidence.scalatest.JsonCodec._
import com.daml.security.evidence.scalatest.JsonCodec.SecurityJson._
import com.daml.security.evidence.scalatest.JsonCodec.ReliabilityJson._

import scala.reflect.ClassTag

object SystematicTestingGenerator {

  private def testNameWithTags(tags: Map[String, Set[String]]): List[(String, List[EvidenceTag])] =
    tags.fmap { tagNames =>
      tagNames.toList
        .filter(_.startsWith("{")) // Check if we have a JSON encoded tag
        .traverse(decode[com.daml.security.evidence.tag.EvidenceTag])
        .valueOr(err => sys.error(s"Failed to parse JSON tag: $err"))
    }.toList

  private def isIgnored(suite: Suite, testName: String): Boolean =
    suite.tags.getOrElse(testName, Set()).contains(Suite.IgnoreTagName)

  private def scalaTestEntries[TT: ClassTag, TS: ClassTag, TE](
      suites: List[Suite],
      testEntry: (String, String, TT, Boolean, Option[TS]) => TE,
  ): List[TE] = {
    suites.flatMap { suite =>
      val testSuite = suite match {
        case testSuite: TS => Some(testSuite)
        case _ => None
      }

      testNameWithTags(suite.tags).mapFilter { case (testName, testTags) =>
        testTags.collectFirst { case testTag: TT =>
          testEntry(suite.suiteName, testName, testTag, isIgnored(suite, testName), testSuite)
        }
      }
    }
  }

  private def testEntries[TT: ClassTag, TS: ClassTag, TE](
      suites: List[LedgerTestSuite],
      testEntry: (String, String, TT, Boolean, Option[TS]) => TE,
  ): List[TE] = {
    suites.flatMap { suite =>
      val testSuite = suite match {
        case testSuite: TS => Some(testSuite)
        case _ => None
      }

      val tags = suite.tests.map { test =>
        test.name -> test.tags
          .map(tag => new com.daml.security.evidence.scalatest.ScalaTestSupport.TagContainer(tag))
          .map(_.name)
          .toSet
      }.toMap

      testNameWithTags(tags).mapFilter { case (testName, testTags) =>
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
    val loader = Runner.getRunpathClassLoader(runpathList)
    val testSuiteNames = SuiteDiscoveryHelper.discoverSuiteNames(runpathList, loader, None)

    println(s"Found #${testSuiteNames.size} test suites, instantiating them:")

    val testSuites = for {
      testSuiteName <- testSuiteNames.toList
      _ = print('.')
      testSuite = DiscoverySuite.getSuiteInstance(testSuiteName, loader)
    } yield testSuite

    println()

    println("Writing security tests inventory..")
    val securityTestsFilePath = File("security-tests.json")
      .write(
        scalaTestEntries[SecurityTest, SecurityTestSuite, SecurityTestEntry](
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
        scalaTestEntries[ReliabilityTest, ReliabilityTestSuite, ReliabilityTestEntry](
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
