// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.security.evidence.tag

trait EvidenceTag

object EvidenceTag {

  /** Convert a multi-line string into a single-line one */
  def singleLine(multiLine: String): String =
    multiLine.linesIterator.map(_.trim).mkString(" ")
}
