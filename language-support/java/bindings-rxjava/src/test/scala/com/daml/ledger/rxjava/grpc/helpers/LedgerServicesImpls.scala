// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.daml.ledger.testkit.services._
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_completion_service.{
  CompletionEndResponse,
  CompletionStreamResponse
}
import com.digitalasset.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse
}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.GetLedgerConfigurationResponse
import com.digitalasset.ledger.api.v1.package_service.{
  GetPackageResponse,
  GetPackageStatusResponse,
  ListPackagesResponse
}
import com.digitalasset.ledger.api.v1.testing.time_service.GetTimeResponse
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import io.reactivex.Observable

import scala.concurrent.{ExecutionContext, Future}

case class LedgerServicesImpls(
    ledgerIdentityServiceImpl: LedgerIdentityServiceImpl,
    activeContractsServiceImpl: ActiveContractsServiceImpl,
    transactionServiceImpl: TransactionServiceImpl,
    commandSubmissionServiceImpl: CommandSubmissionServiceImpl,
    commandCompletionServiceImpl: CommandCompletionServiceImpl,
    commandServiceImpl: CommandServiceImpl,
    ledgerConfigurationServiceImpl: LedgerConfigurationServiceImpl,
    timeServiceImpl: TimeServiceImpl,
    packageServiceImpl: PackageServiceImpl)

object LedgerServicesImpls {

  def createWithRef(
      ledgerId: String,
      getActiveContractsResponse: Observable[GetActiveContractsResponse],
      transactions: Observable[TransactionServiceImpl.LedgerItem],
      commandSubmissionResponse: Future[Empty],
      completions: List[CompletionStreamResponse],
      completionsEnd: CompletionEndResponse,
      submitAndWaitResponse: Future[Empty],
      submitAndWaitForTransactionIdResponse: Future[SubmitAndWaitForTransactionIdResponse],
      submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
      submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse],
      getTimeResponses: List[GetTimeResponse],
      getLedgerConfigurationResponses: Seq[GetLedgerConfigurationResponse],
      listPackagesResponse: Future[ListPackagesResponse],
      getPackageResponse: Future[GetPackageResponse],
      getPackageStatusResponse: Future[GetPackageStatusResponse])(
      implicit ec: ExecutionContext): (Seq[ServerServiceDefinition], LedgerServicesImpls) = {
    val (iServiceDef, iService) = LedgerIdentityServiceImpl.createWithRef(ledgerId)(ec)
    val (acsServiceDef, acsService) =
      ActiveContractsServiceImpl.createWithRef(getActiveContractsResponse)(ec)
    val (tsServiceDef, tsService) =
      TransactionServiceImpl.createWithRef(transactions)(ec)
    val (csServiceDef, csService) =
      CommandSubmissionServiceImpl.createWithRef(commandSubmissionResponse)(ec)
    val (ccServiceDef, ccService) =
      CommandCompletionServiceImpl.createWithRef(completions, completionsEnd)(ec)
    val (cServiceDef, cService) = CommandServiceImpl.createWithRef(
      submitAndWaitResponse,
      submitAndWaitForTransactionIdResponse,
      submitAndWaitForTransactionResponse,
      submitAndWaitForTransactionTreeResponse)(ec)
    val (lcServiceDef, lcService) =
      LedgerConfigurationServiceImpl.createWithRef(getLedgerConfigurationResponses)(ec)
    val (timeServiceDef, timeService) = TimeServiceImpl.createWithRef(getTimeResponses: _*)(ec)
    val (packageServiceDef, packageService) =
      PackageServiceImpl.createWithRef(
        listPackagesResponse,
        getPackageResponse,
        getPackageStatusResponse)(ec)

    val services = Seq(
      iServiceDef,
      acsServiceDef,
      tsServiceDef,
      csServiceDef,
      ccServiceDef,
      cServiceDef,
      lcServiceDef,
      timeServiceDef,
      packageServiceDef)
    val impls = new LedgerServicesImpls(
      iService,
      acsService,
      tsService,
      csService,
      ccService,
      cService,
      lcService,
      timeService,
      packageService)
    (services, impls)
  }
}
