// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc
import com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc.ConfigManagementService
import com.daml.ledger.api.v1.admin.metering_report_service.MeteringReportServiceGrpc
import com.daml.ledger.api.v1.admin.metering_report_service.MeteringReportServiceGrpc.MeteringReportService
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.participant_pruning_service.ParticipantPruningServiceGrpc
import com.daml.ledger.api.v1.admin.participant_pruning_service.ParticipantPruningServiceGrpc.ParticipantPruningService
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc.UserManagementService
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

import scala.annotation.nowarn

private[infrastructure] final class LedgerServices(
    channel: Channel,
    commandInterceptors: Seq[ClientInterceptor],
) {

  val activeContracts: ActiveContractsService =
    ActiveContractsServiceGrpc.stub(channel)

  val command: CommandService =
    CommandServiceGrpc.stub(channel).withInterceptors(commandInterceptors: _*)

  val commandCompletion: CommandCompletionService =
    CommandCompletionServiceGrpc.stub(channel)

  val commandSubmission: CommandSubmissionService =
    CommandSubmissionServiceGrpc.stub(channel).withInterceptors(commandInterceptors: _*)

  val configuration: LedgerConfigurationService =
    LedgerConfigurationServiceGrpc.stub(channel)

  val health: Health =
    HealthGrpc.stub(channel)

  val identity: LedgerIdentityService =
    LedgerIdentityServiceGrpc.stub(channel): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )

  val partyManagement: PartyManagementService =
    PartyManagementServiceGrpc.stub(channel)

  val meteringReport: MeteringReportService =
    MeteringReportServiceGrpc.stub(channel)

  val packageManagement: PackageManagementService =
    PackageManagementServiceGrpc.stub(channel)

  val configManagement: ConfigManagementService =
    ConfigManagementServiceGrpc.stub(channel)

  val participantPruning: ParticipantPruningService =
    ParticipantPruningServiceGrpc.stub(channel)

  val packages: PackageService =
    PackageServiceGrpc.stub(channel)

  val transaction: TransactionService =
    TransactionServiceGrpc.stub(channel)

  val time: TimeService =
    TimeServiceGrpc.stub(channel)

  val version: VersionService =
    VersionServiceGrpc.stub(channel)

  val userManagement: UserManagementService =
    UserManagementServiceGrpc.stub(channel)
}
