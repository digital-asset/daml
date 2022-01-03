// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

/** This package contains interfaces simplifying implementation of a participant server.
  *
  * =Interfaces=
  * The main interfaces that you need to implement to be able to run a participant server are as follows:
  *   - [[com.daml.ledger.participant.state.kvutils.api.LedgerWriter]]: Defines how you submit requests to the ledger
  *     as opaque bytes.
  *   - [[com.daml.ledger.participant.state.kvutils.api.LedgerReader]]: Defines how you read committed key-value pairs
  * from the ledger as opaque bytes.
  *
  * =Running a Participant Server=
  * In order to spin up a participant server there are a few key classes:
  *   - [[com.daml.ledger.participant.state.kvutils.app.LedgerFactory]]:
  *   - [[com.daml.ledger.participant.state.kvutils.app.ReadWriteServiceFactory]]: Defines how you
  *     instantiate your `ReadService` and `WriteService` implementations.
  *   - [[com.daml.ledger.participant.state.kvutils.app.Runner]]: Helper class for spinning up a fully functional
  *     participant server, including the indexer, gRPC interface, etc.
  *   - [[com.daml.ledger.participant.state.kvutils.app.KeyValueReadWriteFactory]]:
  *     [[com.daml.ledger.participant.state.kvutils.app.ReadWriteServiceFactory]] helper implementation,
  *     which uses a [[LedgerReader]] and a [[LedgerWriter]] to instantiate the `ReadService` and the `WriteService`
  * For an example ledger that implements the above interfaces please see the package [[com.daml.ledger.on.memory]].
  *
  * For implementing a validator/committer component please see the below references:
  *   - [[com.daml.ledger.validator.LedgerStateAccess]]
  *   - [[com.daml.ledger.validator.SubmissionValidator]]
  *   - [[com.daml.ledger.validator.ValidatingCommitter]]
  *
  * =Supporting Parallel Submission Validation=
  * In order to support parallel submission validation there are two prerequisites:
  *   - the Participant Server must batch submissions it receives to form `DamlSubmissionBatch`es instead of
  * `DamlSubmission`s (see [[com.daml.ledger.participant.state.kvutils.api.BatchingLedgerWriter]]).
  *    - the `BatchedSubmissionValidator` must be used instead of `SubmissionValidator` in the validator/committer.
  *
  * For implementing a validator/committer component supporting parallel submission validation please see the below
  * references:
  *   - [[com.daml.ledger.validator.LedgerStateOperations]]
  *   - [[com.daml.ledger.validator.batch.BatchedSubmissionValidator]]
  *   - [[com.daml.ledger.validator.batch.BatchedValidatingCommitter]]
  *
  * @see [[com.daml.ledger.participant.state.kvutils.api.LedgerReader]]
  * @see [[com.daml.ledger.participant.state.kvutils.api.LedgerWriter]]
  * @see [[com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantStateReader]]
  * @see [[com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantStateWriter]]
  * @see [[com.daml.ledger.validator.ValidatingCommitter]]
  */
package object api {}
