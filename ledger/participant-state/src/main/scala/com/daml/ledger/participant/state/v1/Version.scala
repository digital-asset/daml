// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

/** This file contains the changelog for the participant state API
  * and version constants (currently none).
  *
  * Changes:
  * [after 100.13.39]:
  * - [[WritePackagesService.uploadPackages]] and [[WritePartyService.allocateParty]]
  *   now return [[SubmissionResult]]. Responses to these requests are communicated
  *   as events, just like with [[WriteService.submitTransaction]].
  *
  * [after 100.13.37]:
  * - Moved configuration serialization from kvutils to participant-state. This is used both by
  *   kvutils and the index to encode and decode configurations.
  * - Authorized participant identifier and "open-world" flag removed from configuration.
  * - Record time added to all [[Update]]s.
  *
  * [after 100.13.21]:
  * - Rename referencedContracts to divulgedContracts in [[Update.TransactionAccepted]].
  */
object Version {}
