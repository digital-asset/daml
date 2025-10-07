// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.suites.v2_1.objectmeta.{
  PartyManagementServiceObjectMetaIT,
  UserManagementServiceObjectMetaIT,
}
import com.digitalasset.canton.config.TlsClientConfig

package object v2_1 {
  def default(timeoutScaleFactor: Double): Vector[LedgerTestSuite] =
    Vector(
      new ActiveContractsServiceIT,
      new CheckpointInTailingStreamsIT,
      new ClosedWorldIT,
      new CommandDeduplicationIT(timeoutScaleFactor),
      new CommandDeduplicationParallelIT,
      new CommandDeduplicationPeriodValidationIT,
      new CommandServiceIT,
      new CommandSubmissionCompletionIT,
      new CompletionDeduplicationInfoIT(CompletionDeduplicationInfoIT.CommandService),
      new CompletionDeduplicationInfoIT(CompletionDeduplicationInfoIT.CommandSubmissionService),
      new ContractIdIT,
      new DamlValuesIT,
      new DeeplyNestedValueIT,
      new DivulgenceIT,
      new EventQueryServiceIT,
      new ExplicitDisclosureIT,
      new HealthServiceIT,
      new IdentityProviderConfigServiceIT,
      new InteractiveSubmissionServiceIT,
      new InterfaceIT,
      new InterfaceSubscriptionsIT,
      new InterfaceSubscriptionsWithEventBlobsIT,
      new LimitsIT,
      new MultiPartySubmissionIT,
      new PackageManagementServiceIT,
      new PackageServiceIT,
      new ParticipantPruningIT,
      new PartyManagementServiceIT,
      new ExternalPartyManagementServiceIT,
      new PartyManagementServiceObjectMetaIT,
      new PartyManagementServiceUpdateRpcIT,
      new SemanticTests,
      new StateServiceIT,
      new TimeServiceIT,
      new TransactionServiceArgumentsIT,
      new TransactionServiceAuthorizationIT,
      new TransactionServiceCorrectnessIT,
      new TransactionServiceExerciseIT,
      new TransactionServiceFiltersIT,
      new TransactionServiceOutputsIT,
      new UpdateServiceQueryIT,
      new TransactionServiceStakeholdersIT,
      new TransactionServiceValidationIT,
      new TransactionServiceVisibilityIT,
      new UpdateServiceStreamsIT,
      new UpdateServiceTopologyEventsIT,
      new UpgradingIT,
      new UserManagementServiceIT,
      new UserManagementServiceObjectMetaIT,
      new UserManagementServiceUpdateRpcIT,
      new ValueLimitsIT,
      new WitnessesIT,
      new WronglyTypedContractIdIT,
      new VettingIT,
    )

  def optional(tlsConfiguration: Option[TlsClientConfig]): Vector[LedgerTestSuite] =
    Vector(
      new TLSOnePointThreeIT(tlsConfiguration),
      new TLSAtLeastOnePointTwoIT(tlsConfiguration),
    )
}
