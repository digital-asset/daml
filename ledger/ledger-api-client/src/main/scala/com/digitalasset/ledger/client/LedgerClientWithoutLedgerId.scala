package com.daml.ledger.client

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc
import com.daml.ledger.client.configuration.LedgerClientConfiguration
import com.daml.ledger.client.services.acs.ActiveContractSetClientWithoutLedgerId
import com.daml.ledger.client.services.admin.{PackageManagementClient, PartyManagementClient}
import com.daml.ledger.client.services.commands.{
  CommandClientWithoutLedgerId,
  SynchronousCommandClient,
}
import com.daml.ledger.client.services.pkg.PackageClientWithoutLedgerId
import com.daml.ledger.client.services.transactions.TransactionClientWithoutLedgerId
import com.daml.ledger.client.services.version.VersionClientWithoutLedgerId
import io.grpc.{Channel, ManagedChannel}
import io.grpc.netty.NettyChannelBuilder

import java.io.Closeable
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext

class LedgerClientWithoutLedgerId private (
    val channel: Channel,
    config: LedgerClientConfiguration,
)(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory)
    extends Closeable {

  val activeContractSetClient =
    new ActiveContractSetClientWithoutLedgerId(
      LedgerClient.stub(ActiveContractsServiceGrpc.stub(channel), config.token)
    )

  val commandClient: CommandClientWithoutLedgerId =
    new CommandClientWithoutLedgerId(
      LedgerClient.stub(CommandSubmissionServiceGrpc.stub(channel), config.token),
      LedgerClient.stub(CommandCompletionServiceGrpc.stub(channel), config.token),
      config.applicationId,
      config.commandClient,
    )

  val commandServiceClient: SynchronousCommandClient =
    new SynchronousCommandClient(LedgerClient.stub(CommandServiceGrpc.stub(channel), config.token))

  val packageClient: PackageClientWithoutLedgerId =
    new PackageClientWithoutLedgerId(
      LedgerClient.stub(PackageServiceGrpc.stub(channel), config.token)
    )

  val packageManagementClient: PackageManagementClient =
    new PackageManagementClient(
      LedgerClient.stub(PackageManagementServiceGrpc.stub(channel), config.token)
    )

  val partyManagementClient: PartyManagementClient =
    new PartyManagementClient(
      LedgerClient.stub(PartyManagementServiceGrpc.stub(channel), config.token)
    )

  val transactionClient: TransactionClientWithoutLedgerId =
    new TransactionClientWithoutLedgerId(
      LedgerClient.stub(TransactionServiceGrpc.stub(channel), config.token)
    )

  val versionClient: VersionClientWithoutLedgerId =
    new VersionClientWithoutLedgerId(
      LedgerClient.stub(VersionServiceGrpc.stub(channel), config.token)
    )

  override def close(): Unit =
    channel match {
      case channel: ManagedChannel =>
        // This includes closing active connections.
        channel.shutdownNow()
        channel.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
        ()
      case _ => // do nothing
    }
}

object LedgerClientWithoutLedgerId {

  def apply(
      channel: Channel,
      config: LedgerClientConfiguration,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory) =
    new LedgerClientWithoutLedgerId(channel, config)

  /** Takes a [[NettyChannelBuilder]], possibly set up with some relevant extra options
    * that cannot be specified though the [[LedgerClientConfiguration]] (e.g. a set of
    * default [[io.grpc.CallCredentials]] to be used with all calls unless explicitly
    * set on a per-call basis), sets the relevant options specified by the configuration
    * (possibly overriding the existing builder settings), and returns a [[LedgerClient]].
    *
    * A shutdown hook is also added to close the channel when the JVM stops.
    */
  def fromBuilder(
      builder: NettyChannelBuilder,
      configuration: LedgerClientConfiguration,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): LedgerClientWithoutLedgerId =
    LedgerClientWithoutLedgerId(GrpcChannel.withShutdownHook(builder, configuration), configuration)
}
