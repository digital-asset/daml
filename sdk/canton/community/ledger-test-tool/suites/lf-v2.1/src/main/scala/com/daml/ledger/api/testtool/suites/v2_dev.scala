// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.digitalasset.canton.config.TlsClientConfig

package object v2_dev {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    v2_1.default(timeoutScaleFactor) ++ Vector(
      new ContractKeysCommandDeduplicationIT,
      new ContractKeysContractIdIT,
      new ContractKeysDeeplyNestedValueIT,
      new ContractKeysDivulgenceIT,
      new ContractKeysExplicitDisclosureIT,
      new ContractKeysIT,
      new ContractKeysMultiPartySubmissionIT,
      new ContractKeysWronglyTypedContractIdIT,
      new EventsDescendantsIT,
      new ExceptionRaceConditionIT,
      new ExceptionsIT,
      new PrefetchContractKeysIT,
      new RaceConditionIT,
    )

  def optional(tlsConfig: Option[TlsClientConfig]): Vector[LedgerTestSuite] =
    v2_1.optional(tlsConfig)
}
