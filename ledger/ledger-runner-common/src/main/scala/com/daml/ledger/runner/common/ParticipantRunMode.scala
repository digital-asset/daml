// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

sealed abstract class ParticipantRunMode

object ParticipantRunMode {

  /** Run the full participant, including both the indexer and ledger API server */
  case object Combined extends ParticipantRunMode {
    override def toString: String = "combined"
  }

  /** Run only the indexer */
  case object Indexer extends ParticipantRunMode {
    override def toString: String = "indexer"
  }

  /** Run only the ledger API server */
  case object LedgerApiServer extends ParticipantRunMode {
    override def toString: String = "ledger-api"
  }

}
