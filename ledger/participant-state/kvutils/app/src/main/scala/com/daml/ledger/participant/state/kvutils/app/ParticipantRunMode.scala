// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

sealed abstract class ParticipantRunMode

object ParticipantRunMode {

  /** Run the full participant, including both the indexer and ledger API server */
  case object Combined extends ParticipantRunMode

  /** Run only the indexer */
  case object Indexer extends ParticipantRunMode

  /** Run only the ledger API server */
  case object LedgerApiServer extends ParticipantRunMode

}
