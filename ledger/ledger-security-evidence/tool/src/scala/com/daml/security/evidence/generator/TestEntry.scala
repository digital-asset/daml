// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.security.evidence.generator

import com.daml.security.evidence.tag.Reliability.{ReliabilityTest, ReliabilityTestSuite}
import com.daml.security.evidence.tag.Security.{SecurityTest, SecurityTestSuite}

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

  final case class ReliabilityTestEntry(
      suiteName: String,
      description: String,
      tag: ReliabilityTest,
      ignored: Boolean,
      suite: Option[ReliabilityTestSuite],
  ) extends TestEntry[ReliabilityTest, ReliabilityTestSuite]
}
