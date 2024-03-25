// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.tag

trait EvidenceTag

object EvidenceTag {

  /** Convert a multi-line string into a single-line one */
  def singleLine(multiLine: String): String =
    multiLine.linesIterator.map(_.trim).mkString(" ")
}
