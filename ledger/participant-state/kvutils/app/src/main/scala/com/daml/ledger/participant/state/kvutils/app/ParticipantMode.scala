// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

sealed abstract class ParticipantMode

object ParticipantMode {

  /** Run the full participant */
  case object Full extends ParticipantMode

  /** Run only the indexer */
  case object Indexer extends ParticipantMode

  /** Run only the ledger API server */
  case object LedgerApiServer extends ParticipantMode

}
