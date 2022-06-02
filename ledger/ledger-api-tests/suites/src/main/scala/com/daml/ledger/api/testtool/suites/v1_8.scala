// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.tls.TlsConfiguration

package object v1_8 {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    Vector(
      new ActiveContractsServiceIT,
      new ClosedWorldIT,
      new CommandDeduplicationIT(timeoutScaleFactor),
      new CommandDeduplicationParallelIT,
      new CommandDeduplicationPeriodValidationIT,
      new CommandServiceIT,
      new CommandSubmissionCompletionIT,
      new CompletionDeduplicationInfoIT(CompletionDeduplicationInfoIT.CommandService),
      new CompletionDeduplicationInfoIT(CompletionDeduplicationInfoIT.CommandSubmissionService),
      new ConfigManagementServiceIT,
      new ContractIdIT,
      new ContractKeysIT,
      new DeeplyNestedValueIT,
      new DivulgenceIT,
      new HealthServiceIT,
      new IdentityIT,
      new LedgerConfigurationServiceIT,
      new MultiPartySubmissionIT,
      new PackageManagementServiceIT,
      new PackageServiceIT,
      new ParticipantPruningIT,
      new PartyManagementServiceIT,
      new RaceConditionIT,
      new SemanticTests,
      new TimeServiceIT,
      new TransactionServiceArgumentsIT,
      new TransactionServiceAuthorizationIT,
      new TransactionServiceCorrectnessIT,
      new TransactionServiceExerciseIT,
      new TransactionServiceOutputsIT,
      new TransactionServiceQueryIT,
      new TransactionServiceStakeholdersIT,
      new TransactionServiceStreamsIT,
      new TransactionServiceValidationIT,
      new TransactionServiceVisibilityIT,
      new UserManagementServiceIT,
      new ValueLimitsIT,
      new WitnessesIT,
      new WronglyTypedContractIdIT,
    )

  def optional(tlsConfiguration: Option[TlsConfiguration]): Vector[LedgerTestSuite] =
    Vector(
      new TLSOnePointThreeIT(tlsConfiguration),
      new TLSAtLeastOnePointTwoIT(tlsConfiguration),
    )
}
