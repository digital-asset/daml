// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}

package object tests {

  val default: Map[String, LedgerSession => LedgerTestSuite] = Map(
    "SemanticTests" -> (new SemanticTests(_))
  )

  /*
   * TODO
   *
   * - TransactionBackpressureIT
   * - CommandTransactionChecksHighLevelIT
   * - CommandTransactionChecksLowLevelIT
   * - CommandSubmissionTtlIT
   */
  val optional: Map[String, LedgerSession => LedgerTestSuite] = Map(
    "ActiveContractsServiceIT" -> (new ActiveContractsService(_)),
    "CommandServiceIT" -> (new CommandService(_)),
    "ContractKeysIT" -> (new ContractKeys(_)),
    "ContractKeysSubmitterIsMaintainerIT" -> (new ContractKeysSubmitterIsMaintainer(_)),
    "DivulgenceIT" -> (new Divulgence(_)),
    "IdentityIT" -> (new Identity(_)),
    "PackageManagementServiceIT" -> (new PackageManagement(_)),
    "PartyManagementServiceIT" -> (new PartyManagement(_)),
    "TimeIT" -> (new Time(_)),
    "TransactionServiceIT" -> (new TransactionService(_)),
    "WitnessesIT" -> (new Witnesses(_)),
  )

  val all = default ++ optional

}
