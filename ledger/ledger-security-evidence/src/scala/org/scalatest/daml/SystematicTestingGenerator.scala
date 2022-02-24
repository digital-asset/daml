// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.scalatest.daml

import better.files.File
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.traverse._
import cats.syntax.functorFilter._
import io.circe.parser.decode
import io.circe.syntax._
import com.daml.ledger.security.test.SystematicTesting
import com.daml.ledger.security.test.SystematicTesting.Reliability.{
  ReliabilityTest,
  ReliabilityTestSuite,
}
import com.daml.ledger.security.test.SystematicTesting.Security.{SecurityTest, SecurityTestSuite}
import com.daml.ledger.security.test.SystematicTesting.TestTag
import io.circe.generic.auto._
import org.scalatest.Suite
import org.scalatest.tools.{DiscoverySuite, Runner, SuiteDiscoveryHelper}

import scala.reflect.ClassTag

/** A test entry in the output. */
trait TestEntry[T, TS] {

  /** suiteName The name of the test suite that this test belongs to. */
  def suiteName: String

  /** The description of the test. */
  def description: String

  /** The test tag to classify the test. */
  def tag: T

  /** Indicate if the test is currently ignored. */
  def ignored: Boolean

  /** Optional test suite data that applies to all tests in the test suite. */
  def suite: Option[TS]
}

case class SecurityTestEntry(
                              suiteName: String,
                              description: String,
                              tag: SecurityTest,
                              ignored: Boolean,
                              suite: Option[SecurityTestSuite],
                            ) extends TestEntry[SecurityTest, SecurityTestSuite]

case class ReliabilityTestEntry(
                                 suiteName: String,
                                 description: String,
                                 tag: ReliabilityTest,
                                 ignored: Boolean,
                                 suite: Option[ReliabilityTestSuite],
                               ) extends TestEntry[ReliabilityTest, ReliabilityTestSuite]

object SystematicTestingGenerator {

  private def testNameWithTags(tags: Map[String, Set[String]]): List[(String, List[TestTag])] =
    tags.fmap { tagNames =>
      tagNames.toList
        .filter(_.startsWith("{")) // Check if we have a JSON encoded tag
        .traverse(decode[SystematicTesting.TestTag])
        .valueOr(err => sys.error(s"Failed to parse JSON tag: $err"))
    }.toList

  private def isIgnored(suite: Suite, testName: String): Boolean =
    suite.tags.getOrElse(testName, Set()).contains(Suite.IgnoreTagName)

  private def testEntries[TT: ClassTag, TS: ClassTag, TE](
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

  private def loadIntelliJClasspath(): Option[String] =
    Some(System.getProperty("java.class.path")).filter(!_.contains("sbt-launch.jar"))

  def main(args: Array[String]): Unit = {
    val cp: Seq[String] = loadIntelliJClasspath().getOrElse(
      sys.error("Currently I only support this to be run in Intellij")
    ).split(":").toSeq
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
    File("/Users/sergeykisel/git/daml/security-tests.json").write(
      testEntries[SecurityTest, SecurityTestSuite, SecurityTestEntry](
        testSuites,
        SecurityTestEntry,
      ).asJson.spaces2
    )

    println("Writing reliability tests inventory..")
    File("/Users/sergeykisel/git/daml/reliability-tests.json").write(
      testEntries[ReliabilityTest, ReliabilityTestSuite, ReliabilityTestEntry](
        testSuites,
        ReliabilityTestEntry,
      ).asJson.spaces2
    )

    sys.exit()
  }
}
