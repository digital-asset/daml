// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
   * - TransactionServiceIT
   * - TransactionBackpressureIT
   * - CommandTransactionChecksHighLevelIT
   * - CommandTransactionChecksLowLevelIT
   * - PackageManagementServiceIT
   * - PartyManagementServiceIT
   * - CommandSubmissionTtlIT
   * - CommandServiceIT
   * - ActiveContractsServiceIT
   * - WitnessesIT
   * - ContractKeysIT
   */
  val optional: Map[String, LedgerSession => LedgerTestSuite] = Map(
    "DivulgenceIT" -> (new Divulgence(_)),
    "IdentityIT" -> (new Identity(_)),
    "TimeIT" -> (new Time(_)),
    "ContractKeysIT" -> (new ContractKeys(_)),
  )

  val all = default ++ optional

}
