// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc
import com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc.ConfigManagementService
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.participant_pruning_service.ParticipantPruningServiceGrpc
import com.daml.ledger.api.v1.admin.participant_pruning_service.ParticipantPruningServiceGrpc.ParticipantPruningService
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc.VersionService
import io.grpc.{Channel, ClientInterceptor}
import io.grpc.health.v1.health.HealthGrpc
import io.grpc.health.v1.health.HealthGrpc.Health

private[infrastructure] final class LedgerServices(
    participant: Channel,
    commandInterceptors: Seq[ClientInterceptor],
) {

  val activeContracts: ActiveContractsService =
    ActiveContractsServiceGrpc.stub(participant)

  val command: CommandService =
    CommandServiceGrpc.stub(participant).withInterceptors(commandInterceptors: _*)

  val commandCompletion: CommandCompletionService =
    CommandCompletionServiceGrpc.stub(participant)

  val commandSubmission: CommandSubmissionService =
    CommandSubmissionServiceGrpc.stub(participant).withInterceptors(commandInterceptors: _*)

  val configuration: LedgerConfigurationService =
    LedgerConfigurationServiceGrpc.stub(participant)

  val health: Health =
    HealthGrpc.stub(participant)

  val identity: LedgerIdentityService =
    LedgerIdentityServiceGrpc.stub(participant)

  val partyManagement: PartyManagementService =
    PartyManagementServiceGrpc.stub(participant)

  val packageManagement: PackageManagementService =
    PackageManagementServiceGrpc.stub(participant)

  val configManagement: ConfigManagementService =
    ConfigManagementServiceGrpc.stub(participant)

  val participantPruning: ParticipantPruningService =
    ParticipantPruningServiceGrpc.stub(participant)

  val packages: PackageService =
    PackageServiceGrpc.stub(participant)

  val transaction: TransactionService =
    TransactionServiceGrpc.stub(participant)

  val time: TimeService =
    TimeServiceGrpc.stub(participant)

  val version: VersionService =
    VersionServiceGrpc.stub(participant)
}
