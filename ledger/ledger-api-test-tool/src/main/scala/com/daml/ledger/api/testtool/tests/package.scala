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
   */
  val optional: Map[String, LedgerSession => LedgerTestSuite] = Map(
    "DivulgenceIT" -> (new Divulgence(_)),
    "IdentityIT" -> (new Identity(_)),
    "TimeIT" -> (new Time(_)),
    // FixMe: https://github.com/digital-asset/daml/issues/1866
    // add back if contract key restriction is enable
    // "ContractKeysSubmitterIsMaintainerIT" -> (new ContractKeysSubmitterIsMaintainer(_)),
    "ContractKeysIT" -> (new ContractKeys(_)),
    "WitnessesIT" -> (new Witnesses(_))
  )

  val all = default ++ optional

}
