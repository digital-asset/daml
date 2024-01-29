// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.scalatest.daml

import org.scalatest.{DeferredAbortedSuite, Suite}
import org.scalatest.tools.{DiscoverySuite, Runner, SuiteDiscoveryHelper}

/*
 * This adapter is required to expose internal package-protected Scala Test library
 * methods to discover Suite instances and `IgnoreTagName`.
 */
object ScalaTestAdapter {

  val IgnoreTagName: String = Suite.IgnoreTagName

  def loadTestSuites(runpathList: List[String], fatalWarnings: Boolean = false): List[Suite] = {
    val loader = Runner.getRunpathClassLoader(runpathList)
    val testSuiteNames = SuiteDiscoveryHelper.discoverSuiteNames(runpathList, loader, None)
    val suites = for {
      testSuiteName <- testSuiteNames.toList
    } yield DiscoverySuite.getSuiteInstance(testSuiteName, loader)

    val abortedSuites = suites.collect { case DeferredAbortedSuite(suiteClassName, throwable) =>
      Console.err.println(s"warning: Could not load suite $suiteClassName.")
      throwable.printStackTrace()
      suiteClassName
    }
    if (fatalWarnings && abortedSuites.nonEmpty) {
      sys.error(s"Could not load test suites: ${abortedSuites.mkString(", ")}")
    } else {
      suites
    }
  }

}
