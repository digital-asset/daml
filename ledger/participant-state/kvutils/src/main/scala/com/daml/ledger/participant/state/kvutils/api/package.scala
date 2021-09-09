// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.lf.data.Ref
import com.daml.telemetry.TelemetryContext

import scala.concurrent.Future

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
  *   - [[com.daml.ledger.participant.state.kvutils.app.LedgerFactory.KeyValueLedgerFactory]]: Defines how you
  *   instantiate your `LedgerReader` and `LedgerWriter` implementations.
  *   - [[com.daml.ledger.participant.state.kvutils.app.Runner]]: Helper class for spinning up a fully functional
  *    participant server, including the indexer, gRPC interface, etc.
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
  * @see [[com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState]]
  * @see [[com.daml.ledger.validator.ValidatingCommitter]]
  */
package object api {
  type KeyValueLedger = LedgerReader with LedgerWriter

  def createKeyValueLedger(reader: LedgerReader, writer: LedgerWriter): KeyValueLedger =
    new LedgerReader with LedgerWriter {

      override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
        reader.events(startExclusive)

      override def ledgerId(): LedgerId = reader.ledgerId()

      override def currentHealth(): HealthStatus =
        reader.currentHealth().and(writer.currentHealth())

      override def participantId: Ref.ParticipantId = writer.participantId

      override def commit(
          correlationId: String,
          envelope: Raw.Envelope,
          metadata: CommitMetadata,
      )(implicit telemetryContext: TelemetryContext): Future[SubmissionResult] =
        writer.commit(correlationId, envelope, metadata)
    }
}
