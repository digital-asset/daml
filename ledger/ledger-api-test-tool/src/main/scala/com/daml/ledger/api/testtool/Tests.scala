// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.daml.ledger.api.testtool.tests._

object Tests {
  type Tests = Map[String, LedgerSession => LedgerTestSuite]

  val default: Tests = Map(
    "SemanticTests" -> (new SemanticTests(_)),
  )

  /*
   * TODO
   *
   * - CommandTransactionChecksHighLevelIT
   * - CommandTransactionChecksLowLevelIT
   * - CommandSubmissionTtlIT
   */
  val optional: Tests = Map(
    "ActiveContractsServiceIT" -> (new ActiveContractsService(_)),
    "CommandServiceIT" -> (new CommandService(_)),
    "CommandSubmissionCompletionIT" -> (new CommandSubmissionCompletion(_)),
    "ContractKeysIT" -> (new ContractKeys(_)),
    "ContractKeysSubmitterIsMaintainerIT" -> (new ContractKeysSubmitterIsMaintainer(_)),
    "DivulgenceIT" -> (new Divulgence(_)),
    "HealthServiceIT" -> (new HealthService(_)),
    "IdentityIT" -> (new Identity(_)),
    "LedgerConfigurationServiceIT" -> (new LedgerConfigurationService(_)),
    "LotsOfPartiesIT" -> (new LotsOfParties(_)),
    "PackageManagementServiceIT" -> (new PackageManagement(_)),
    "PackageServiceIT" -> (new Packages(_)),
    "PartyManagementServiceIT" -> (new PartyManagement(_)),
    "TimeIT" -> (new Time(_)),
    "TransactionServiceIT" -> (new TransactionService(_)),
    "TransactionScaleIT" -> (new TransactionScale(_)),
    "WitnessesIT" -> (new Witnesses(_)),
    "WronglyTypedContractIdIT" -> (new WronglyTypedContractId(_)),
    "ConfigManagementServiceIT" -> (new ConfigManagement(_)),
  )

  val all: Tests = default ++ optional
}
