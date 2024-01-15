// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.tag

/** A tag for a missing test case.
  *
  * Similar to a to-do entry for test coverage but can be included in the inventory.
  * Alternatively one can use `ignore` with the appropriate tag already applied.
  */
final case class MissingTest(missingCoverage: String) extends EvidenceTag
