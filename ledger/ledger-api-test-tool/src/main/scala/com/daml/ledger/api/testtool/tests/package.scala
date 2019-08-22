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
   * - TransactionServiceIT
   * - TransactionBackpressureIT
   * - CommandTransactionChecksHighLevelIT
   * - CommandTransactionChecksLowLevelIT
   * - CommandSubmissionTtlIT
   */
  val optional: Map[String, LedgerSession => LedgerTestSuite] = Map(
    "DivulgenceIT" -> (new Divulgence(_)),
    "IdentityIT" -> (new Identity(_)),
    "TimeIT" -> (new Time(_)),
    // FixMe: https://github.com/digital-asset/daml/issues/2289
    //   enable once the testing dar is compiled using 1.dev
    //   (see ledger/test-common/BUILD.bazel)
    //   "ContractKeysSubmitterIsMaintainerIT" -> (new ContractKeysSubmitterIsMaintainer(_)),
    "ContractKeysIT" -> (new ContractKeys(_)),
    "WitnessesIT" -> (new Witnesses(_)),
    "CommandServiceIT" -> (new CommandService(_)),
    "ActiveContractsServiceIT" -> (new ActiveContractsService(_)),
    "PartyManagementServiceIT" -> (new PartyManagement(_)),
    "PackageManagementServiceIT" -> (new PackageManagement(_))
  )

  val all = default ++ optional

}
