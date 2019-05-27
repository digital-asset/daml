// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import org.scalatest.events.Event
import org.scalatest.DistributedTestSorter

/** Noop-sorter. Used primarily to disable scalatest's buggy reporter mechanism.
  * See https://github.com/digital-asset/daml/issues/1243 for more details.
  */
class ToolSorter extends DistributedTestSorter {
  override def distributingTest(testName: String): Unit = ()
  override def apply(testName: String, event: Event): Unit = ()
  override def completedTest(testName: String): Unit = ()
}
