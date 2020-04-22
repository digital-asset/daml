// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

/** This file defines the version of the protocol buffer messages,
  * and the changelog of kvutils.
  *
  * Changes:
  * [after 100.13.55]: *BACKWARDS INCOMPATIBLE*
  * - Remove use of relative contract ids. Introduces kvutils version 2.
  * - Introduce DamlSubmissionBatch.
  * - Respect the deduplication time provided by submissions.
  *   - Remove DamlCommandDedupKey#application_id.
  *   - Add DamlCommanDedupValue#deduplicatedUntil.
  *   - Introduces kvutils version 3.
  *
  * [after 100.13.52]: *BACKWARDS INCOMPATIBLE*
  * - Use hash for serializing contract keys instead of serializing the value, as
  *   the value serialization is not guaranteed to be stable over time.
  *
  * [after 100.13.39]:
  * - logEntryToAsyncResponse has now been removed as all requests
  *   now have event based responses.
  *
  * [after 100.13.37]:
  * - Removed DamlConfiguration in favour of participant-state's LedgerConfiguration.
  * - Authorization of configuration changes is now based on validating against the participant id
  *   of the previously submitted configuration.
  *
  * [after 100.13.29]:
  * - Add support for ledger dumps via environment variable: "KVUTILS_LEDGER_DUMP=/tmp/ledger.dump".
  * - Add integrity checker tool to verify ledger dumps for validating compatibility of new versions.
  *
  * [after 100.13.26]:
  * - Added metrics to track submission processing.
  * - Use InsertOrdMap to store resulting state in kvutils for deterministic ordering of state key-values.
  * - Fix bug with transient contract keys, e.g. keys created and archived in same transaction.
  *
  * [after 100.13.21]:
  * - Added 'Envelope' for compressing and versioning kvutils messages that are transmitted
  *   or stored on disk. [[Envelope.enclose]] and [[Envelope.open]] should be now used for
  *   submissions and for results from processing them.
  * - Disabled the time-to-live checks for ledger effective time and record time. The
  *   time model is being redesigned and the checks will be reimplemented once we have
  *   the new design.
  *
  * [after 100.13.16]: *BACKWARDS INCOMPATIBLE*
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

  /** The kvutils version number. Packing kvutils messages into envelopes carries the version number.
    * Version should be incremented when semantics of fields change and migration of data is required or
    * when the protobuf default value for a field is insufficient and must be filled in during decoding.
    * Handling of older versions is handled by [[Envelope.open]] which performs the migration to latest version.
    *
    * Version history:
    *   0: * Initial version
    *
    *   1: * Use hashing to serialize contract keys. Backwards incompatible to avoid having to do two lookups
    *        of a single contract key.
    *
    *   2: * Deprecate use of relative contract identifiers. The transaction is submitted with absolute contract
    *        identifiers. Backwards incompatible to remove unnecessary traversal of the transaction when consuming
    *        it and to make it possible to remove DamlLogEntryId.
    *
    *   3: * Add an explicit deduplication time window to each submission. Backwards incompatible because
    *        it is unclear how to set a sensible default value if the submission time is unknown.
    *      * Add submissionTime in DamlTransactionEntry and use it instead of ledgerTime to derive
    *        contract ids.
    *      * Add DamlSubmissionBatch message.
    *   4: * Remove application_id from DamlCommandDedupKey. Only submitter and commandId are used for deduplication.
    *      * Add deduplicatedUntil field to DamlCommandDedupValue to restrict the deduplication window.
    *   5: * Add active_at to DamlContractKeyState to be able to check causal monotonicity of positive key lookups,
    *        i.e. whether the contract currently associated with a contract key was created in a transaction with
    *        ledger_effective_time <= the ledger_effective_time of the transaction under validation.
    *
    */
  val version: Long = 5
}
