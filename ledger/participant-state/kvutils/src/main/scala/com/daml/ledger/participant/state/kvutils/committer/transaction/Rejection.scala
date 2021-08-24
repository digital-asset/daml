// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import java.time.Instant

import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey
import com.daml.ledger.participant.state.kvutils.Err
import com.daml.lf
import com.daml.lf.data.Ref

sealed trait Rejection {
  def description: String
}

object Rejection {
  final case class ValidationFailure(error: lf.engine.Error) extends Rejection {
    override lazy val description: String =
      error.message
  }

  object InternallyInconsistentTransaction {
    object DuplicateKeys extends Rejection {
      override val description: String =
        "DuplicateKeys: the transaction contains a duplicate key"
    }

    object InconsistentKeys extends Rejection {
      override val description: String =
        "InconsistentKeys: the transaction is internally inconsistent"

    }
  }

  object ExternallyInconsistentTransaction {
    object InconsistentContracts extends Rejection {
      override def description: String =
        "InconsistentContracts: at least one contract has been archived since the submission"

    }

    object DuplicateKeys extends Rejection {
      override val description: String =
        "DuplicateKeys: at least one contract key is not unique"

    }

    object InconsistentKeys extends Rejection {
      override val description: String =
        "InconsistentKeys: at least one contract key has changed since the submission"

    }
  }

  final case class MissingInputState(key: DamlStateKey) extends Rejection {
    override lazy val description: String =
      s"Missing input state for key $key"

  }

  final case class InvalidParticipantState(error: Err) extends Rejection {
    override lazy val description: String =
      error.getMessage

  }

  final case class LedgerTimeOutOfRange(
      outOfRange: LedgerTimeModel.OutOfRange
  ) extends Rejection {
    override lazy val description: String =
      outOfRange.message

  }

  final case class RecordTimeOutOfRange(
      minimumRecordTime: Instant,
      maximumRecordTime: Instant,
  ) extends Rejection {
    override lazy val description: String =
      s"Record time is outside of valid range [$minimumRecordTime, $maximumRecordTime]"

  }

  object CausalMonotonicityViolated extends Rejection {
    override val description: String =
      "Causal monotonicity violated"

  }

  final case class SubmittingPartyNotKnownOnLedger(submitter: Ref.Party) extends Rejection {
    override lazy val description: String =
      s"Submitting party '$submitter' not known"

  }

  final case class PartiesNotKnownOnLedger(parties: Iterable[Ref.Party]) extends Rejection {
    override lazy val description: String =
      s"Parties not known on ledger: ${parties.mkString("[", ", ", "]")}"

  }

  final case class SubmitterCannotActViaParticipant(
      submitter: Ref.Party,
      participantId: Ref.ParticipantId,
  ) extends Rejection {
    override lazy val description: String =
      s"Party '$submitter' not hosted by participant $participantId"

  }
}
