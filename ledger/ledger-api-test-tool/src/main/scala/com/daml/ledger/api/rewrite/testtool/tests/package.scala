// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool

import com.daml.ledger.api.rewrite.testtool.infrastructure.{LedgerSession, LedgerTestSuite}

package object tests {

  val default: Map[String, LedgerSession => LedgerTestSuite] = Map(
    "SemanticTests" -> (new SemanticTests(_))
  )

  /*
   * TODO
   *
   * - transactionServiceIT
   * - transactionBackpressureIT
   * - commandTransactionChecksHighLevelIT
   * - commandTransactionChecksLowLevelIT
   * - packageManagementServiceIT
   * - partyManagementServiceIT
   * - commandSubmissionTtlIT
   * - commandServiceIT
   * - activeContractsServiceIT
   * - witnessesIT
   * - contractKeysIT
   */
  val optional: Map[String, LedgerSession => LedgerTestSuite] = Map(
    "DivulgenceIT" -> (new Divulgence(_)),
    "Identity" -> (new Identity(_)),
    "Time" -> (new Time(_)),
  )

  val all = default ++ optional

}
