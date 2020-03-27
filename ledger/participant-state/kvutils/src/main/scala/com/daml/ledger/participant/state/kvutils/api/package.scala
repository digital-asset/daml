// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

/**
  * This package contains interfaces simplifying implementation of a participant server.
  *
  * =Interfaces=
  * The main interfaces that you need to implement to be able to run a participant server are as follows:
  *   - [[com.daml.ledger.participant.state.kvutils.api.LedgerWriter]]: Defines how you submit requests to the ledger
  * as opaque bytes.
  *   - [[com.daml.ledger.participant.state.kvutils.api.LedgerReader]]: Defines how you read committed key-value pairs
  * from the ledger as opaque bytes.
  *
  * =Running a Participant Server=
  * In order to spin up a participant server there are a few key classes:
  *   - [[com.daml.ledger.participant.state.kvutils.app.LedgerFactory.KeyValueLedgerFactory]]: Defines how you
  *   instantiate your `LedgerReader` and `LedgerWriter` implementations.
  *   - [[com.daml.ledger.participant.state.kvutils.app.Runner]]: Helper class for spinning up a fully functional
  *    participant server, including the indexer, gRPC interface, etc.
  * For an example ledger that implements the above interfaces please see the package [[com.daml.ledger.on.memory]].
  *
  * For implementing a validator/committer component please see the below references.
  *
  * @see [[com.daml.ledger.validator.LedgerStateAccess]]
  * @see [[com.daml.ledger.validator.SubmissionValidator]]
  * @see [[com.daml.ledger.validator.ValidatingCommitter]]
  */
package object api {}
