// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.generator

import com.daml.test.evidence.tag.Reliability.{ReliabilityTest, ReliabilityTestSuite}
import com.daml.test.evidence.tag.Security.{SecurityTest, SecurityTestSuite}

/** A test entry in the output. */
sealed trait TestEntry[T, TS] {

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

object TestEntry {
  final case class SecurityTestEntry(
      suiteName: String,
      description: String,
      tag: SecurityTest,
      ignored: Boolean,
      suite: Option[SecurityTestSuite],
  ) extends TestEntry[SecurityTest, SecurityTestSuite]

  object SecurityTestEntry {
    implicit val ordering: Ordering[SecurityTestEntry] =
      Ordering[(String, String, Int)].on[SecurityTestEntry] { entry =>
        (entry.suiteName, entry.tag.file, entry.tag.line)
      }
  }

  final case class ReliabilityTestEntry(
      suiteName: String,
      description: String,
      tag: ReliabilityTest,
      ignored: Boolean,
      suite: Option[ReliabilityTestSuite],
  ) extends TestEntry[ReliabilityTest, ReliabilityTestSuite]

  object ReliabilityTestEntry {
    implicit val ordering: Ordering[ReliabilityTestEntry] =
      Ordering[(String, String, Int)].on[ReliabilityTestEntry] { entry =>
        (entry.suiteName, entry.tag.file, entry.tag.line)
      }
  }
}
