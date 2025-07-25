// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.canton.auth.Authorizer
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForReassignmentResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitResponse,
}
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v2.package_service.{
  GetPackageResponse,
  GetPackageStatusResponse,
  ListPackagesResponse,
}
import com.daml.ledger.api.v2.testing.time_service.GetTimeResponse
import com.daml.ledger.api.v2.command_submission_service.SubmitResponse
import io.grpc.ServerServiceDefinition
import io.reactivex.Observable

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

case class LedgerServicesImpls(
    stateServiceImpl: StateServiceImpl,
    transactionServiceImpl: UpdateServiceImpl,
    commandSubmissionServiceImpl: CommandSubmissionServiceImpl,
    commandCompletionServiceImpl: CommandCompletionServiceImpl,
    commandServiceImpl: CommandServiceImpl,
    timeServiceImpl: TimeServiceImpl,
    eventQueryServiceImpl: EventQueryServiceImpl,
    packageServiceImpl: PackageServiceImpl,
)

object LedgerServicesImpls {

  @nowarn(
    "cat=deprecation"
  ) // use submitAndWaitForTransaction instead of submitAndWaitForTransactionTree
  def createWithRef(
      getActiveContractsResponse: Observable[GetActiveContractsResponse],
      transactions: Observable[UpdateServiceImpl.LedgerItem],
      commandSubmissionResponse: Future[SubmitResponse],
      completions: List[CompletionStreamResponse],
      submitAndWaitResponse: Future[SubmitAndWaitResponse],
      submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
      submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse],
      submitAndWaitForReassignmentResponse: Future[SubmitAndWaitForReassignmentResponse],
      getTimeResponse: Future[GetTimeResponse],
      getEventsByContractIdResponse: Future[GetEventsByContractIdResponse],
      listPackagesResponse: Future[ListPackagesResponse],
      getPackageResponse: Future[GetPackageResponse],
      getPackageStatusResponse: Future[GetPackageStatusResponse],
      authorizer: Authorizer,
  )(implicit ec: ExecutionContext): (Seq[ServerServiceDefinition], LedgerServicesImpls) = {
    val (stateServiceDef, stateService) =
      StateServiceImpl.createWithRef(getActiveContractsResponse, transactions, authorizer)(ec)
    val (tsServiceDef, tsService) =
      UpdateServiceImpl.createWithRef(transactions, authorizer)(ec)
    val (csServiceDef, csService) =
      CommandSubmissionServiceImpl.createWithRef(() => commandSubmissionResponse, authorizer)(ec)
    val (ccServiceDef, ccService) =
      CommandCompletionServiceImpl.createWithRef(completions, authorizer)(ec)
    val (cServiceDef, cService) = CommandServiceImpl.createWithRef(
      submitAndWaitResponse,
      submitAndWaitForTransactionResponse,
      submitAndWaitForTransactionTreeResponse,
      submitAndWaitForReassignmentResponse,
      authorizer,
    )(ec)
    val (timeServiceDef, timeService) =
      TimeServiceImpl.createWithRef(getTimeResponse, authorizer)(ec)
    val (eventQueryServiceDef, eventQueryService) =
      EventQueryServiceImpl.createWithRef(
        getEventsByContractIdResponse,
        authorizer,
      )(ec)
    val (packageServiceDef, packageService) =
      PackageServiceImpl.createWithRef(
        listPackagesResponse,
        getPackageResponse,
        getPackageStatusResponse,
        authorizer,
      )(ec)

    val services = Seq(
      stateServiceDef,
      tsServiceDef,
      csServiceDef,
      ccServiceDef,
      cServiceDef,
      timeServiceDef,
      eventQueryServiceDef,
      packageServiceDef,
    )
    val impls = new LedgerServicesImpls(
      stateService,
      tsService,
      csService,
      ccService,
      cService,
      timeService,
      eventQueryService,
      packageService,
    )
    (services, impls)
  }
}
