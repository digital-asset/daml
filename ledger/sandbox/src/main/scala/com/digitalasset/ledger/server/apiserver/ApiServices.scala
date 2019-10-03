// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.server.apiserver

import akka.stream.ActorMaterializer
import com.daml.ledger.participant.state.index.v2.{
  IdentityProvider,
  IndexActiveContractsService,
  IndexConfigurationService,
  IndexPackagesService,
  _
}
import com.daml.ledger.participant.state.v1.{AuthService, TimeModel, WriteService}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.engine._
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionEndRequest
import com.digitalasset.ledger.client.services.commands.CommandSubmissionFlow
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.server.api.authorization.services._
import com.digitalasset.platform.sandbox.config.CommandConfiguration
import com.digitalasset.platform.sandbox.services._
import com.digitalasset.platform.sandbox.services.admin.ApiPackageManagementService
import com.digitalasset.platform.sandbox.services.transaction.ApiTransactionService
import com.digitalasset.platform.sandbox.services.admin.ApiPartyManagementService
import com.digitalasset.platform.sandbox.stores.ledger.CommandExecutorImpl
import com.digitalasset.platform.server.services.command.ApiCommandService
import com.digitalasset.platform.server.services.identity.ApiLedgerIdentityService
import com.digitalasset.platform.server.services.testing.{ApiTimeService, TimeServiceBackend}

import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService
import scalaz.syntax.tag._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

trait ApiServices extends AutoCloseable {
  val services: Iterable[BindableService]

  def withServices(otherServices: immutable.Seq[BindableService]): ApiServices
}

private case class ApiServicesBundle(services: immutable.Seq[BindableService]) extends ApiServices {

  override def close(): Unit =
    services.foreach {
      case closeable: AutoCloseable => closeable.close()
      case _ => ()
    }

  override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices =
    copy(services = services.++:(otherServices))

}

object ApiServices {

  def create(
      writeService: WriteService,
      indexService: IndexService,
      authService: AuthService,
      engine: Engine,
      timeProvider: TimeProvider,
      timeModel: TimeModel,
      commandConfig: CommandConfiguration,
      optTimeServiceBackend: Option[TimeServiceBackend],
      loggerFactory: NamedLoggerFactory)(
      implicit mat: ActorMaterializer,
      esf: ExecutionSequencerFactory): Future[ApiServices] = {
    implicit val ec: ExecutionContext = mat.system.dispatcher

    // still trying to keep it tidy in case we want to split it later
    val configurationService: IndexConfigurationService = indexService
    val identityService: IdentityProvider = indexService
    val packagesService: IndexPackagesService = indexService
    val activeContractsService: IndexActiveContractsService = indexService
    val transactionsService: IndexTransactionsService = indexService
    val contractStore: ContractStore = indexService
    val completionsService: IndexCompletionsService = indexService
    val partyManagementService: IndexPartyManagementService = indexService

    identityService.getLedgerId().map { ledgerId =>
      val apiSubmissionService =
        ApiSubmissionService.create(
          ledgerId,
          contractStore,
          writeService,
          timeModel,
          timeProvider,
          new CommandExecutorImpl(engine, packagesService.getLfPackage),
          loggerFactory
        )

      loggerFactory.getLogger(this.getClass).info(EngineInfo.show)

      val apiTransactionService =
        ApiTransactionService.create(ledgerId, transactionsService, loggerFactory)

      val apiLedgerIdentityService =
        ApiLedgerIdentityService.create(() => identityService.getLedgerId(), loggerFactory)

      val apiPackageService = ApiPackageService.create(ledgerId, packagesService, loggerFactory)

      val apiConfigurationService =
        ApiLedgerConfigurationService.create(ledgerId, configurationService, loggerFactory)

      val apiCompletionService =
        ApiCommandCompletionService
          .create(ledgerId, completionsService, loggerFactory)

      val apiCommandService = ApiCommandService.create(
        ApiCommandService.Configuration(
          ledgerId,
          commandConfig.inputBufferSize,
          commandConfig.maxParallelSubmissions,
          commandConfig.maxCommandsInFlight,
          commandConfig.limitMaxCommandsInFlight,
          commandConfig.historySize,
          commandConfig.retentionPeriod,
          commandConfig.commandTtl
        ),
        // Using local services skips the gRPC layer, improving performance.
        ApiCommandService.LowLevelCommandServiceAccess.LocalServices(
          CommandSubmissionFlow(apiSubmissionService.submit, commandConfig.maxParallelSubmissions),
          r => apiCompletionService.completionStreamSource(r),
          () => apiCompletionService.completionEnd(CompletionEndRequest(ledgerId.unwrap)),
          apiTransactionService.getTransactionById,
          apiTransactionService.getFlatTransactionById
        ),
        loggerFactory
      )

      val apiActiveContractsService =
        ApiActiveContractsService.create(ledgerId, activeContractsService, loggerFactory)

      val apiReflectionService = ProtoReflectionService.newInstance()

      val apiTimeServiceOpt =
        optTimeServiceBackend.map { tsb =>
          ApiTimeService.create(
            ledgerId,
            tsb,
            loggerFactory
          )
        }

      val apiPartyManagementService =
        ApiPartyManagementService
          .createApiService(partyManagementService, writeService, loggerFactory)

      val apiPackageManagementService =
        ApiPackageManagementService.createApiService(indexService, writeService, loggerFactory)

      // Note: the command service uses the command submission, command completion, and transaction services internally.
      // These connections do not use authorization, authorization wrappers are only added here to all exposed services.
      new ApiServicesBundle(
        apiTimeServiceOpt.toList :::
          List(
          new LedgerIdentityServiceAuthorization(apiLedgerIdentityService, authService),
          new PackageServiceAuthorization(apiPackageService, authService),
          new LedgerConfigurationServiceAuthorization(apiConfigurationService, authService),
          new CommandSubmissionServiceAuthorization(apiSubmissionService, authService),
          new TransactionServiceAuthorization(apiTransactionService, authService),
          new CommandCompletionServiceAuthorization(apiCompletionService, authService),
          new CommandServiceAuthorization(apiCommandService, authService),
          new ActiveContractsServiceAuthorization(apiActiveContractsService, authService),
          apiReflectionService,
          new PartyManagementServiceAuthorization(apiPartyManagementService, authService),
          new PackageManagementServiceAuthorization(apiPackageManagementService, authService),
        ))
    }
  }

}
