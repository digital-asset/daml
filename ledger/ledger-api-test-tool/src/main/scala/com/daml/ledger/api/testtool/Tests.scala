// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.daml.ledger.api.testtool.tests._

object Tests {
  type Tests = Map[String, LedgerSession => LedgerTestSuite]

  /**
    * These tests are safe to be run concurrently and are
    * always run by default, unless otherwise specified.
    */
  val default: Tests = Map(
    "ActiveContractsServiceIT" -> (new ActiveContractsService(_)),
    "CommandServiceIT" -> (new CommandService(_)),
    "CommandSubmissionCompletionIT" -> (new CommandSubmissionCompletion(_)),
    "CommandDeduplicationIT" -> (new CommandDeduplication(_)),
    "ContractKeysIT" -> (new ContractKeys(_)),
    "ContractKeysSubmitterIsMaintainerIT" -> (new ContractKeysSubmitterIsMaintainer(_)),
    "DivulgenceIT" -> (new Divulgence(_)),
    "HealthServiceIT" -> (new HealthService(_)),
    "IdentityIT" -> (new Identity(_)),
    "LedgerConfigurationServiceIT" -> (new LedgerConfigurationService(_)),
    "PackageManagementServiceIT" -> (new PackageManagement(_)),
    "PackageServiceIT" -> (new Packages(_)),
    "PartyManagementServiceIT" -> (new PartyManagement(_)),
    "SemanticTests" -> (new SemanticTests(_)),
    "TransactionServiceIT" -> (new TransactionService(_)),
    "WitnessesIT" -> (new Witnesses(_)),
    "WronglyTypedContractIdIT" -> (new WronglyTypedContractId(_)),
    "ClosedWorldIT" -> (new ClosedWorld(_)),
  )

  /**
    * These tests can:
    * - change the global state of the ledger, or
    * - be especially resource intensive (by design)
    *
    * These are consequently not run unless otherwise specified.
    */
  val optional: Tests = Map(
    "ConfigManagementServiceIT" -> (new ConfigManagement(_)),
    "LotsOfPartiesIT" -> (new LotsOfParties(_)),
    "TransactionScaleIT" -> (new TransactionScale(_)),
  )

  val all: Tests = default ++ optional
}
