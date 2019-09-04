// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

/** This file defines the version of the protocol buffer messages,
  * and the changelog of kvutils.
  *
  * Changes:
  * [since 100.13.21]:
  * - Added 'Envelope' for compressing and versioning kvutils messages that are transmitted
  *   or stored on disk. [[Envelope.enclose]] and [[Envelope.open]] should be now used for
  *   submissions and for results from processing them.
  * - Disabled the time-to-live checks for ledger effective time and record time. The
  *   time model is being redesigned and the checks will be reimplemented once we have
  *   the new design.
  *
  * [since 100.13.16]: *BACKWARDS INCOMPATIBLE*
  * - Log entries are no longer used as inputs to submission processing. The
  *   contract instance is now stored within DamlContractStatus.
  * - Configuration extended with "Open World" flag that defines whether
  *   submissions from unallocated parties are accepted.
  * - Support for authenticating submissions based on participant id. The
  *   [[KeyValueCommitting.processSubmission]] method now takes the participant id as
  *   argument.
  * - Support for submitting authenticated configuration changes.
  * - Bug in command deduplication fixed: rejected commands are now deduplicated correctly.
  */
object Version {
  val version: Long = 0
}
