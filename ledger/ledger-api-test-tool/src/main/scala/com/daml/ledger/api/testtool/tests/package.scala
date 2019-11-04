// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}

package object tests {
  type Tests = Map[String, LedgerSession => LedgerTestSuite]

  val default: Tests = Map(
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
  val optional: Tests = Map(
    "ActiveContractsServiceIT" -> (new ActiveContractsService(_)),
    "CommandServiceIT" -> (new CommandService(_)),
    "ContractKeysIT" -> (new ContractKeys(_)),
    "ContractKeysSubmitterIsMaintainerIT" -> (new ContractKeysSubmitterIsMaintainer(_)),
    "DivulgenceIT" -> (new Divulgence(_)),
    "IdentityIT" -> (new Identity(_)),
    "LedgerConfigurationServiceIT" -> (new LedgerConfigurationService(_)),
    "LotsOfPartiesIT" -> (new LotsOfParties(_)),
    "PackageManagementServiceIT" -> (new PackageManagement(_)),
    "PackageServiceIT" -> (new Packages(_)),
    "PartyManagementServiceIT" -> (new PartyManagement(_)),
    "TimeIT" -> (new Time(_)),
    "TransactionServiceIT" -> (new TransactionService(_)),
    "WitnessesIT" -> (new Witnesses(_)),
    "WronglyTypedContractIdIT" -> (new WronglyTypedContractId(_))
  )

  val all: Tests = default ++ optional
}
