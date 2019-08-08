package com.daml.ledger.participant.state.kvutils

/** This file defines the version of the protocol buffer messages,
  * and the changelog of kvutils.
  *
  * Changes:
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

  // FIXME(JM): Introduce versioned messages.
  // final val protoVersion: Long = 0
}
