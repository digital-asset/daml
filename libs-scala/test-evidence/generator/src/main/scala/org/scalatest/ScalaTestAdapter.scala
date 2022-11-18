// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  def loadTestSuites(runpathList: List[String]): List[Suite] = {
    val loader = Runner.getRunpathClassLoader(runpathList)
    val testSuiteNames = SuiteDiscoveryHelper.discoverSuiteNames(runpathList, loader, None)
    println(s"runpath: ${runpathList.mkString(",")}")
    println(s"suites: ${testSuiteNames.mkString("\n")}")
    val suites = for {
      testSuiteName <- testSuiteNames.toList
    } yield DiscoverySuite.getSuiteInstance(testSuiteName, loader)

    suites.tapEach {
      case DeferredAbortedSuite(suiteClassName, throwable) =>
        Console.err.println(
          s"warning: Could not load suite $suiteClassName.\n   ${throwable.getCause}\n"
        )
      case _ =>
    }
  }

}
