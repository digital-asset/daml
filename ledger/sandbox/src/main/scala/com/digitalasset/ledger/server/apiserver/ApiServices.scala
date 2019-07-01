// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.ledger.participant.state.v2.WriteService
import com.daml.ledger.participant.state.v2.TimeModel
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.engine._
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionEndRequest
import com.digitalasset.ledger.client.services.commands.CommandSubmissionFlow
import com.digitalasset.platform.sandbox.config.CommandConfiguration
import com.digitalasset.platform.sandbox.services._
import com.digitalasset.platform.sandbox.services.admin.ApiPackageManagementService
import com.digitalasset.platform.sandbox.services.transaction.ApiTransactionService
import com.digitalasset.platform.sandbox.services.admin.ApiPartyManagementService
import com.digitalasset.platform.sandbox.stores.ledger.CommandExecutorImpl
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.digitalasset.platform.server.services.command.ApiCommandService
import com.digitalasset.platform.server.services.identity.ApiLedgerIdentityService
import com.digitalasset.platform.server.services.testing.{ApiTimeService, TimeServiceBackend}
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService
import org.slf4j.LoggerFactory
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

  private val logger = LoggerFactory.getLogger(this.getClass)

  def create(
      writeService: WriteService,
      indexService: IndexService,
      engine: Engine,
      timeProvider: TimeProvider,
      timeModel: TimeModel,
      commandConfig: CommandConfiguration,
      optTimeServiceBackend: Option[TimeServiceBackend])(
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
      val identifierResolver: IdentifierResolver =
        new IdentifierResolver(packagesService.getLfPackage)

      val apiSubmissionService =
        ApiSubmissionService.create(
          ledgerId,
          identifierResolver,
          contractStore,
          writeService,
          timeModel,
          timeProvider,
          new CommandExecutorImpl(engine, packagesService.getLfPackage)
        )

      logger.info(EngineInfo.show)

      val apiTransactionService =
        ApiTransactionService.create(ledgerId, transactionsService, identifierResolver)

      val apiLedgerIdentityService =
        ApiLedgerIdentityService.create(() => identityService.getLedgerId())

      val apiPackageService = ApiPackageService.create(ledgerId, packagesService)

      val apiConfigurationService =
        ApiLedgerConfigurationService.create(ledgerId, configurationService)

      val apiCompletionService =
        ApiCommandCompletionService
          .create(ledgerId, completionsService)

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
        identifierResolver
      )

      val apiActiveContractsService =
        ApiActiveContractsService.create(ledgerId, activeContractsService, identifierResolver)

      val apiReflectionService = ProtoReflectionService.newInstance()

      val apiTimeServiceOpt =
        optTimeServiceBackend.map { tsb =>
          ApiTimeService.create(
            ledgerId,
            tsb
          )
        }

      val apiPartyManagementService =
        ApiPartyManagementService
          .createApiService(partyManagementService, writeService)

      val apiPackageManagementService =
        ApiPackageManagementService.createApiService(indexService, writeService)

      new ApiServicesBundle(
        apiTimeServiceOpt.toList :::
          List(
          apiLedgerIdentityService,
          apiPackageService,
          apiConfigurationService,
          apiSubmissionService,
          apiTransactionService,
          apiCompletionService,
          apiCommandService,
          apiActiveContractsService,
          apiReflectionService,
          apiPartyManagementService,
          apiPackageManagementService
        ))
    }
  }

}
