package com.digitalasset.platform.apitesting

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.testing.reset_service.ResetServiceGrpc.ResetService
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import io.grpc.reflection.v1alpha.ServerReflectionGrpc

class MultiLedgerContext(val mapping: Map[Party, LedgerContext],
                         val defaultParty: Party,
                         implicit val _esf: ExecutionSequencerFactory) extends LedgerContext {

  private lazy val default: LedgerContext = forParty(defaultParty)

  override protected implicit def esf: ExecutionSequencerFactory = _esf

  override def forParty(party: Party): LedgerContext =
    mapping.get(party) match {
      case Some(lc) => lc
      case None => throw new IllegalArgumentException("Unrecognised party: "+party+", expecting one of: "+mapping.keySet.toString)
    }

  override def ledgerId: String = default.ledgerId
  override def packageIds: Iterable[Ref.PackageId] = default.packageIds
  override def ledgerIdentityService: LedgerIdentityService = default.ledgerIdentityService
  override def ledgerConfigurationService: LedgerConfigurationService = default.ledgerConfigurationService
  override def packageService: PackageService = default.packageService
  override def commandSubmissionService: CommandSubmissionService = default.commandSubmissionService
  override def commandCompletionService: CommandCompletionService = default.commandCompletionService
  override def commandService: CommandService = default.commandService
  override def transactionService: TransactionService = default.transactionService
  override def timeService: TimeService = default.timeService
  override def acsService: ActiveContractsService = default.acsService
  override def transactionClient: TransactionClient = default.transactionClient
  override def packageClient: PackageClient = default.packageClient
  override def acsClient: ActiveContractSetClient = default.acsClient
  override def reflectionService: ServerReflectionGrpc.ServerReflectionStub = default.reflectionService
  override def resetService: ResetService = default.resetService
}
