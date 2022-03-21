// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.scalatest.daml

import org.scalatest.Suite
import org.scalatest.tools.{DiscoverySuite, Runner, SuiteDiscoveryHelper}

object ScalaTestAdapter {

  val IgnoreTagName: String = Suite.IgnoreTagName

  def loadTestSuites(runpathList: List[String]): List[Suite] = {
    val loader = Runner.getRunpathClassLoader(runpathList)
    val testSuiteNames = SuiteDiscoveryHelper.discoverSuiteNames(runpathList, loader, None)
    for {
      testSuiteName <- testSuiteNames.toList
    } yield DiscoverySuite.getSuiteInstance(testSuiteName, loader)
  }

}
