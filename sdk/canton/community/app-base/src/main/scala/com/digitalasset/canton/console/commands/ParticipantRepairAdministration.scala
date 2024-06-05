// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import better.files.File
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.admin.api.client.commands.{
  GrpcAdminCommand,
  ParticipantAdminCommands,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  CommandErrors,
  CommandSuccessful,
  ConsoleCommandResult,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.participant.ParticipantNodeCommon
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.v0.{
  AcsSnapshotChunk,
  DownloadRequest,
  ExportAcsRequest,
  ExportAcsResponse,
}
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.protocol.{LfContractId, SerializableContractWithWitnesses}
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, DomainAlias, SequencerCounter}
import com.google.protobuf.ByteString
import io.grpc.Context.CancellableContext
import io.grpc.StatusRuntimeException

import java.time.Instant
import java.util.UUID
import scala.concurrent.{Await, Promise, TimeoutException}

class ParticipantRepairAdministration(
    val consoleEnvironment: ConsoleEnvironment,
    runner: AdminCommandRunner,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with NoTracing
    with Helpful {

  @Help.Summary("Purge contracts with specified Contract IDs from local participant.")
  @Help.Description(
    """This is a last resort command to recover from data corruption, e.g. in scenarios in which participant
      |contracts have somehow gotten out of sync and need to be manually purged, or in situations in which
      |stakeholders are no longer available to agree to their archival. The participant needs to be disconnected from
      |the domain on which the contracts with "contractIds" reside at the time of the call, and as of now the domain
      |cannot have had any inflight requests.
      |The effects of the command will take affect upon reconnecting to the sync domain.
      |The "ignoreAlreadyPurged" flag makes it possible to invoke the command multiple times with the same
      |parameters in case an earlier command invocation has failed.
      |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
      |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
      |configuration. In addition repair commands can run for an unbounded time depending on the number of
      |contract ids passed in. Be sure to not connect the participant to the domain until the call returns.
      |
      |Arguments are:
      |  - inputFile: the path to the file containing the ACS export
      |  - offboardedParties: the list of parties that will be offboarded after the purge."""
  )
  def purge(
      domain: DomainAlias,
      contractIds: Seq[LfContractId],
      offboardedParties: Set[PartyId],
      ignoreAlreadyPurged: Boolean = true,
  ): Unit =
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.ParticipantRepairManagement.PurgeContracts(
          domain = domain,
          contracts = contractIds,
          ignoreAlreadyPurged = ignoreAlreadyPurged,
          offboardedParties = offboardedParties,
        )
      )
    }

  @Help.Summary("Migrate contracts from one domain to another one.")
  @Help.Description(
    """This method can be used to migrate all the contracts associated with a domain to a new domain connection.
         This method will register the new domain, connect to it and then re-associate all contracts on the source
         domain to the target domain. Please note that this migration needs to be done by all participants
         at the same time. The domain should only be used once all participants have finished their migration.

         The arguments are:
         source: the domain alias of the source domain
         target: the configuration for the target domain
         """
  )
  def migrate_domain(
      source: DomainAlias,
      target: DomainConnectionConfig,
  ): Unit = {
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.ParticipantRepairManagement.MigrateDomain(source, target)
      )
    }
  }

  @Help.Summary("Download all contracts for the given set of parties to a file.")
  @Help.Description(
    """This command can be used to download the current active contract set of a given set of parties to a text file.
        |This is mainly interesting for recovery and operational purposes.
        |
        |The file will contain base64 encoded strings, one line per contract. The lines are written
        |sorted according to their domain and contract id. This allows to compare the contracts stored
        |by two participants using standard file comparison tools.
        |The domain-id is printed with the prefix domain-id before the block of contracts starts.
        |
        |This command may take a long time to complete and may require significant resources.
        |It will first load the contract ids of the active contract set into memory and then subsequently
        |load the contracts in batches and inspect their stakeholders. As this operation needs to traverse
        |the entire datastore, it might take a long time to complete.
        |
        |The command will return a map of domainId -> number of active contracts stored
        |
        The arguments are:
        - parties: identifying contracts having at least one stakeholder from the given set
        - partiesOffboarding: true if the parties will be offboarded (party migration)
        - outputFile: the output file name where to store the data. Use .gz as a suffix to get a compressed file (recommended)
        - filterDomainId: restrict the export to a given domain
        - timestamp: optionally a timestamp for which we should take the state (useful to reconcile states of a domain)
        - protocolVersion: optional the protocol version to use for the serialization. Defaults to the one of the domains.
        - chunkSize: size of the byte chunks to stream back: default 1024 * 1024 * 2 = (2MB)
        - contractDomainRenames: As part of the export, allow to rename the associated domain id of contracts from one domain to another based on the mapping.
        """
  )
  @deprecated(
    "Use export_acs",
    since = "2.8.0",
  ) // TODO(i14441): Remove deprecated ACS download / upload functionality
  def download(
      parties: Set[PartyId],
      partiesOffboarding: Boolean,
      outputFile: String = ParticipantRepairAdministration.DefaultFile,
      filterDomainId: String = "",
      timestamp: Option[Instant] = None,
      protocolVersion: Option[ProtocolVersion] = None,
      chunkSize: Option[PositiveInt] = None,
      contractDomainRenames: Map[DomainId, DomainId] = Map.empty,
  ): Unit = {
    check(FeatureFlag.Repair) {
      val generator = AcsSnapshotFileCollector[DownloadRequest, AcsSnapshotChunk](outputFile)
      val command = ParticipantAdminCommands.ParticipantRepairManagement
        .Download(
          parties,
          partiesOffboarding = partiesOffboarding,
          filterDomainId,
          timestamp,
          protocolVersion,
          chunkSize,
          generator.observer,
          generator.hasGzipExtension,
          contractDomainRenames,
        )

      generator.materializeFile(command)
    }
  }

  @Help.Summary("Export active contracts for the given set of parties to a file.")
  @Help.Description(
    """This command exports the current Active Contract Set (ACS) of a given set of parties to ACS snapshot file.
        |Afterwards, the 'import_acs' command allows importing it into a participant's ACS again.
        |Such ACS export (and import) is interesting for recovery and operational purposes only.
        |Note that the 'export_acs' command execution may take a long time to complete and may require significant
        |resources.
        |
        |The arguments are:
        |- parties: identifying contracts having at least one stakeholder from the given set
        |- partiesOffboarding: true if the parties will be offboarded (party migration)
        |- outputFile: the output file name where to store the data. Use .gz as a suffix to get a compressed file (recommended)
        |- filterDomainId: restrict the export to a given domain
        |- timestamp: optionally a timestamp for which we should take the state (useful to reconcile states of a domain)
        |- contractDomainRenames: As part of the export, allow to rename the associated domain id of contracts from one domain to another based on the mapping.
        """
  )
  def export_acs(
      parties: Set[PartyId],
      partiesOffboarding: Boolean,
      outputFile: String = ParticipantRepairAdministration.ExportAcsDefaultFile,
      filterDomainId: Option[DomainId] = None,
      timestamp: Option[Instant] = None,
      contractDomainRenames: Map[DomainId, (DomainId, ProtocolVersion)] = Map.empty,
  ): Unit = {
    check(FeatureFlag.Repair) {
      val collector = AcsSnapshotFileCollector[ExportAcsRequest, ExportAcsResponse](outputFile)
      val command = ParticipantAdminCommands.ParticipantRepairManagement
        .ExportAcs(
          parties,
          partiesOffboarding = partiesOffboarding,
          filterDomainId,
          timestamp,
          collector.observer,
          contractDomainRenames,
        )
      collector.materializeFile(command)
    }
  }

  private case class AcsSnapshotFileCollector[
      Req,
      Resp <: GrpcByteChunksToFileObserver.ByteStringChunk,
  ](outputFile: String) {
    private val target = File(outputFile)
    private val requestComplete = Promise[String]()
    val observer = new GrpcByteChunksToFileObserver[Resp](
      target,
      requestComplete,
    )
    private val timeout = consoleEnvironment.commandTimeouts.ledgerCommand
    val hasGzipExtension: Boolean = target.toJava.getName.endsWith(".gz")

    def materializeFile(
        command: GrpcAdminCommand[
          Req,
          CancellableContext,
          CancellableContext,
        ]
    ): Unit = {
      consoleEnvironment.run {

        def call = consoleEnvironment.run {
          runner.adminCommand(
            command
          )
        }

        try {
          ResourceUtil.withResource(call) { _ =>
            CommandSuccessful(
              Await
                .result(
                  requestComplete.future,
                  timeout.duration,
                )
                .discard
            )
          }
        } catch {
          case sre: StatusRuntimeException =>
            GenericCommandError(
              GrpcError("Generating acs snapshot file", "download_acs_snapshot", sre).toString
            )
          case _: TimeoutException =>
            target.delete(swallowIOExceptions = true)
            CommandErrors.ConsoleTimeout.Error(timeout.asJavaApproximation)
        }
      }
    }
  }

  @Help.Summary("Import ACS snapshot")
  @Help.Description("""Uploads a binary into the participant's ACS""")
  @deprecated(
    "Use import_acs",
    since = "2.8.0",
  ) // TODO(i14441): Remove deprecated ACS download / upload functionality
  def upload(
      inputFile: String = ParticipantRepairAdministration.DefaultFile
  ): Unit = {
    check(FeatureFlag.Repair) {
      val file = File(inputFile)
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.Upload(
            ByteString.copyFrom(file.loadBytes),
            file.extension().contains(".gz"),
          )
        )
      }
    }
  }

  @Help.Summary("Import active contracts from an Active Contract Set (ACS) snapshot file.")
  @Help.Description(
    """This command imports contracts from an ACS snapshot file into the participant's ACS.
        |The given ACS snapshot file needs to be the resulting file from a previous 'export_acs' command invocation.
        |
        |Arguments are:
        |  - inputFile: the path to the file containing the ACS export
        |  - onboardedParties: the list of new parties whose contracts are imported
        """
  )
  def import_acs(
      inputFile: String = ParticipantRepairAdministration.ExportAcsDefaultFile,
      onboardedParties: Set[PartyId],
      workflowIdPrefix: String = "",
  ): Unit = {
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.ImportAcs(
            ByteString.copyFrom(File(inputFile).loadBytes),
            workflowIdPrefix =
              if (workflowIdPrefix.nonEmpty) workflowIdPrefix
              else s"import-${UUID.randomUUID}",
            onboardedParties = onboardedParties,
          )
        )
      }
    }
  }

  @Help.Summary("Add specified contracts to specific domain on local participant.")
  @Help.Description(
    """This is a last resort command to recover from data corruption, e.g. in scenarios in which participant
        |contracts have somehow gotten out of sync and need to be manually created. The participant needs to be
        |disconnected from the specified "domain" at the time of the call, and as of now the domain cannot have had
        |any inflight requests.
        |The effects of the command will take affect upon reconnecting to the sync domain.
        |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
        |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
        |configuration. In addition repair commands can run for an unbounded time depending on the number of
        |contracts passed in. Be sure to not connect the participant to the domain until the call returns.
        |
        The arguments are:
        - domainId: the id of the domain to which to add the contract
        - protocolVersion: to protocol version used by the domain
        - contractsToAdd: list of contracts to add with witness information
        """
  )
  def add(
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      contractsToAdd: Seq[SerializableContractWithWitnesses],
      onboardedParties: Set[PartyId],
  ): Unit = {

    val temporaryFile = File.newTemporaryFile(suffix = ".gz")
    val outputStream = temporaryFile.newGzipOutputStream()

    ResourceUtil.withResource(outputStream) { outputStream =>
      contractsToAdd
        .traverse_ { contract =>
          val activeContractE = ActiveContract
            .create(domainId, contract.contract)(
              protocolVersion
            )
            .leftMap(_.toString)

          activeContractE.flatMap(_.writeDelimitedTo(outputStream).map(_ => outputStream.flush()))
        }
        .valueOr(err => throw new RuntimeException(s"Unable to add contract data to stream: $err"))
    }

    val bytes = ByteString.copyFrom(temporaryFile.loadBytes)
    temporaryFile.delete(swallowIOExceptions = true)

    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.ImportAcs(
            bytes,
            workflowIdPrefix = s"import-${UUID.randomUUID}",
            onboardedParties = onboardedParties,
          )
        )
      }
    }
  }

  @Help.Summary("Purge select data of a deactivated domain.")
  @Help.Description(
    """This command deletes selected domain data and helps to ensure that stale data in the specified, deactivated domain
       |is not acted upon anymore. The specified domain needs to be in the `Inactive` status for purging to occur.
       |Purging a deactivated domain is typically performed automatically as part of a hard domain migration via
       |``repair.migrate_domain``."""
  )
  def purge_deactivated_domain(domain: DomainAlias): Unit = {
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.PurgeDeactivatedDomain(domain)
        )
      }
    }
  }

}

abstract class LocalParticipantRepairAdministration(
    override val consoleEnvironment: ConsoleEnvironment,
    runner: AdminCommandRunner,
    override val loggerFactory: NamedLoggerFactory,
) extends ParticipantRepairAdministration(
      consoleEnvironment = consoleEnvironment,
      runner = runner,
      loggerFactory = loggerFactory,
    ) {

  protected def access[T](handler: ParticipantNodeCommon => T): T

  @Help.Summary("Add specified contracts to specific domain on local participant.")
  @Help.Description(
    """This is a last resort command to recover from data corruption, e.g. in scenarios in which participant
        |contracts have somehow gotten out of sync and need to be manually created. The participant needs to be
        |disconnected from the specified "domain" at the time of the call, and as of now the domain cannot have had
        |any inflight requests.
        |For each "contractsToAdd", specify "witnesses", local parties, in case no local party is a stakeholder.
        |The "ignoreAlreadyAdded" flag makes it possible to invoke the command multiple times with the same
        |parameters in case an earlier command invocation has failed.
        |
        |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
        |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
        |configuration. In addition repair commands can run for an unbounded time depending on the number of
        |contracts passed in. Be sure to not connect the participant to the domain until the call returns.
        |
        The arguments are:
        - domain: the alias of the domain to which to add the contract
        - contractsToAdd: list of contracts to add with witness information
        - ignoreAlreadyAdded: (default true) if set to true, it will ignore contracts that already exist on the target domain.
        - ignoreStakeholderCheck: (default false) if set to true, add will work for contracts that don't have a local party (useful for party migration).
        """
  )
  def add(
      domain: DomainAlias,
      contractsToAdd: Seq[SerializableContractWithWitnesses],
      ignoreAlreadyAdded: Boolean = true,
      ignoreStakeholderCheck: Boolean = false,
  ): Unit =
    runRepairCommand(tc =>
      access(
        _.sync.repairService
          .addContracts(
            domain,
            contractsToAdd,
            ignoreAlreadyAdded = ignoreAlreadyAdded,
            ignoreStakeholderCheck = ignoreStakeholderCheck,
            hostedParties = None,
          )(tc)
      )
    )

  private def runRepairCommand[T](command: TraceContext => Either[String, T]): T =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        ConsoleCommandResult.fromEither {
          // Ensure that admin repair commands have a non-empty trace context.
          TraceContext.withNewTraceContext(command(_))
        }
      }
    }

  @Help.Summary("Move contracts with specified Contract IDs from one domain to another.")
  @Help.Description(
    """This is a last resort command to recover from data corruption in scenarios in which a domain is
        |irreparably broken and formerly connected participants need to move contracts to another, healthy domain.
        |The participant needs to be disconnected from both the "sourceDomain" and the "targetDomain". Also as of now
        |the target domain cannot have had any inflight requests.
        |Contracts already present in the target domain will be skipped, and this makes it possible to invoke this
        |command in an "idempotent" fashion in case an earlier attempt had resulted in an error.
        |The "skipInactive" flag makes it possible to only move active contracts in the "sourceDomain".
        |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
        |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
        |configuration. In addition repair commands can run for an unbounded time depending on the number of
        |contract ids passed in. Be sure to not connect the participant to either domain until the call returns.

        Arguments:
        - contractIds - set of contract ids that should be moved to the new domain
        - sourceDomain - alias of the source domain
        - targetDomain - alias of the target domain
        - skipInactive - (default true) whether to skip inactive contracts mentioned in the contractIds list
        - batchSize - (default 100) how many contracts to write at once to the database"""
  )
  def change_domain(
      contractIds: Seq[LfContractId],
      sourceDomain: DomainAlias,
      targetDomain: DomainAlias,
      skipInactive: Boolean = true,
      batchSize: Int = 100,
  ): Unit =
    runRepairCommand(tc =>
      access(
        _.sync.repairService.changeDomainAwait(
          contractIds,
          sourceDomain,
          targetDomain,
          skipInactive,
          PositiveInt.tryCreate(batchSize),
        )(tc)
      )
    )

  @Help.Summary("Mark sequenced events as ignored.")
  @Help.Description(
    """This is the last resort to ignore events that the participant is unable to process.
      |Ignoring events may lead to subsequent failures, e.g., if the event creating a contract is ignored and
      |that contract is subsequently used. It may also lead to ledger forks if other participants still process
      |the ignored events.
      |It is possible to mark events as ignored that the participant has not yet received.
      |
      |The command will fail, if marking events between `from` and `to` as ignored would result in a gap in sequencer counters,
      |namely if `from <= to` and `from` is greater than `maxSequencerCounter + 1`,
      |where `maxSequencerCounter` is the greatest sequencer counter of a sequenced event stored by the underlying participant.
      |
      |The command will also fail, if `force == false` and `from` is smaller than the sequencer counter of the last event
      |that has been marked as clean.
      |(Ignoring such events would normally have no effect, as they have already been processed.)"""
  )
  def ignore_events(
      domainId: DomainId,
      from: SequencerCounter,
      to: SequencerCounter,
      force: Boolean = false,
  ): Unit =
    runRepairCommand(tc =>
      access {
        _.sync.repairService.ignoreEvents(domainId, from, to, force)(tc)
      }
    )

  @Help.Summary("Remove the ignored status from sequenced events.")
  @Help.Description(
    """This command has no effect on ordinary (i.e., not ignored) events and on events that do not exist.
      |
      |The command will fail, if marking events between `from` and `to` as unignored would result in a gap in sequencer counters,
      |namely if there is one empty ignored event with sequencer counter between `from` and `to` and
      |another empty ignored event with sequencer counter greater than `to`.
      |An empty ignored event is an event that has been marked as ignored and not yet received by the participant.
      |
      |The command will also fail, if `force == false` and `from` is smaller than the sequencer counter of the last event
      |that has been marked as clean.
      |(Unignoring such events would normally have no effect, as they have already been processed.)"""
  )
  def unignore_events(
      domainId: DomainId,
      from: SequencerCounter,
      to: SequencerCounter,
      force: Boolean = false,
  ): Unit =
    runRepairCommand(tc =>
      access {
        _.sync.repairService.unignoreEvents(domainId, from, to, force)(tc)
      }
    )
}

object ParticipantRepairAdministration {
  private val DefaultFile = "canton-acs-snapshot.gz"
  private val ExportAcsDefaultFile = "canton-acs-export.gz"
}
