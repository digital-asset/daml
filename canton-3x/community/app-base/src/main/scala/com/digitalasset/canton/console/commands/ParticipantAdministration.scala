// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.option.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Pruning.{
  GetParticipantScheduleCommand,
  SetParticipantScheduleCommand,
}
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Resources.{
  GetResourceLimits,
  SetResourceLimits,
}
import com.digitalasset.canton.admin.api.client.commands.{
  DomainTimeCommands,
  LedgerApiCommands,
  ParticipantAdminCommands,
  PruningSchedulerCommands,
}
import com.digitalasset.canton.admin.api.client.data.{
  DarMetadata,
  ListConnectedDomainsResult,
  ParticipantPruningSchedule,
}
import com.digitalasset.canton.admin.participant.v0
import com.digitalasset.canton.admin.participant.v0.PruningServiceGrpc
import com.digitalasset.canton.admin.participant.v0.PruningServiceGrpc.PruningServiceStub
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DomainTimeTrackerConfig, NonNegativeDuration}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  BaseInspection,
  CommandFailure,
  ConsoleEnvironment,
  ConsoleMacros,
  DomainReference,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReferenceWithSequencerConnection,
  LedgerApiCommandRunner,
  ParticipantReferenceCommon,
}
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.ParticipantStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.ParticipantNodeCommon
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.admin.grpc.TransferSearchResult
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.platform.apiserver.services.ApiConversions
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{
  LfCommittedTransaction,
  LfContractId,
  SerializableContract,
  TransferId,
}
import com.digitalasset.canton.sequencing.{
  PossiblyIgnoredProtocolEvent,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.{
  DiscardOps,
  DomainAlias,
  LedgerApplicationId,
  SequencerAlias,
  config,
}

import java.time.Instant
import java.util.UUID
import scala.concurrent.TimeoutException
import scala.concurrent.duration.Duration

sealed trait DomainChoice
object DomainChoice {
  object All extends DomainChoice
  final case class Only(aliases: Seq[DomainAlias]) extends DomainChoice
}

private[console] object ParticipantCommands {

  object dars {

    def upload(
        runner: AdminCommandRunner,
        path: String,
        vetAllPackages: Boolean,
        synchronizeVetting: Boolean,
        logger: TracedLogger,
    ) =
      runner.adminCommand(
        ParticipantAdminCommands.Package
          .UploadDar(Some(path), vetAllPackages, synchronizeVetting, logger)
      )

  }

  object domains {

    def referenceToConfig(
        domain: NonEmpty[Map[SequencerAlias, InstanceReferenceWithSequencerConnection]],
        manualConnect: Boolean = false,
        alias: Option[DomainAlias] = None,
        maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
        priority: Int = 0,
        sequencerTrustThreshold: PositiveInt = PositiveInt.tryCreate(1),
    ): DomainConnectionConfig = {
      val domainAlias = alias.getOrElse(
        DomainAlias.tryCreate(domain.head1._2.name)
      ) // TODO(#14048): Come up with a good way of giving it a good alias
      DomainConnectionConfig(
        domainAlias,
        SequencerConnections.tryMany(
          domain.toSeq.map { case (alias, domain) =>
            domain.sequencerConnection.withAlias(alias)
          },
          sequencerTrustThreshold,
        ),
        manualConnect = manualConnect,
        None,
        priority,
        None,
        maxRetryDelay,
        DomainTimeTrackerConfig(),
      )
    }

    def toConfig(
        domainAlias: DomainAlias,
        connection: String,
        manualConnect: Boolean = false,
        domainId: Option[DomainId] = None,
        certificatesPath: String = "",
        priority: Int = 0,
        initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
        maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
        timeTrackerConfig: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
    ): DomainConnectionConfig = {
      // architecture-handbook-entry-begin: OnboardParticipantToConfig
      val certificates = OptionUtil.emptyStringAsNone(certificatesPath).map { path =>
        BinaryFileUtil.readByteStringFromFile(path) match {
          case Left(err) => throw new IllegalArgumentException(s"failed to load ${path}: ${err}")
          case Right(bs) => bs
        }
      }
      DomainConnectionConfig.grpc(
        SequencerAlias.Default,
        domainAlias,
        connection,
        manualConnect,
        domainId,
        certificates,
        priority,
        initialRetryDelay,
        maxRetryDelay,
        timeTrackerConfig,
      )
      // architecture-handbook-entry-end: OnboardParticipantToConfig
    }

    def register(runner: AdminCommandRunner, config: DomainConnectionConfig) =
      runner.adminCommand(
        ParticipantAdminCommands.DomainConnectivity.RegisterDomain(config)
      )
    def reconnect(runner: AdminCommandRunner, domainAlias: DomainAlias, retry: Boolean) = {
      runner.adminCommand(
        ParticipantAdminCommands.DomainConnectivity.ConnectDomain(domainAlias, retry)
      )
    }

    def list_connected(runner: AdminCommandRunner) =
      runner.adminCommand(
        ParticipantAdminCommands.DomainConnectivity.ListConnectedDomains()
      )

    def reconnect_all(runner: AdminCommandRunner, ignoreFailures: Boolean) =
      runner.adminCommand(
        ParticipantAdminCommands.DomainConnectivity.ReconnectDomains(ignoreFailures)
      )

    def disconnect(runner: AdminCommandRunner, domainAlias: DomainAlias) =
      runner.adminCommand(ParticipantAdminCommands.DomainConnectivity.DisconnectDomain(domainAlias))

  }
}

class ParticipantTestingGroup(
    participantRef: ParticipantReferenceCommon,
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with Helpful {
  import participantRef.*

  @Help.Summary(
    "Send a bong to a set of target parties over the ledger. Levels > 0 leads to an exploding ping with exponential number of contracts. " +
      "Throw a RuntimeException in case of failure.",
    FeatureFlag.Testing,
  )
  @Help.Description(
    """Initiates a racy ping to multiple participants,
     measuring the roundtrip time of the fastest responder, with an optional timeout.
     Grace-period is the time the bong will wait for a duplicate spent (which would indicate an error in the system) before exiting.
     If levels > 0, the ping command will lead to a binary explosion and subsequent dilation of
     contracts, where ``level`` determines the number of levels we will explode. As a result, the system will create
     (2^(L+2) - 3) contracts (where L stands for ``level``).
     Normally, only the initiator is a validator. Additional validators can be added using the validators argument.
     The bong command comes handy to run a burst test against the system and quickly leads to an overloading state."""
  )
  def bong(
      targets: Set[ParticipantId],
      validators: Set[ParticipantId] = Set(),
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.testingBong,
      levels: Long = 0,
      gracePeriodMillis: Long = 1000,
      workflowId: String = "",
      id: String = "",
  ): Duration = {
    consoleEnvironment.runE(
      maybe_bong(targets, validators, timeout, levels, gracePeriodMillis, workflowId, id)
        .toRight(
          s"Unable to bong $targets with $levels levels within ${LoggerUtil.roundDurationForHumans(timeout.duration)}"
        )
    )
  }

  @Help.Summary("Like bong, but returns None in case of failure.", FeatureFlag.Testing)
  def maybe_bong(
      targets: Set[ParticipantId],
      validators: Set[ParticipantId] = Set(),
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.testingBong,
      levels: Long = 0,
      gracePeriodMillis: Long = 1000,
      workflowId: String = "",
      id: String = "",
  ): Option[Duration] =
    check(FeatureFlag.Testing)(consoleEnvironment.run {
      adminCommand(
        ParticipantAdminCommands.Ping
          .Ping(
            targets.map(_.adminParty.toLf),
            validators.map(_.adminParty.toLf),
            timeout.duration.toMillis,
            levels,
            gracePeriodMillis,
            workflowId,
            id,
          )
      )
    })

  @Help.Summary("Fetch the current time from the given domain", FeatureFlag.Testing)
  def fetch_domain_time(
      domainAlias: DomainAlias,
      timeout: NonNegativeDuration,
  ): CantonTimestamp =
    check(FeatureFlag.Testing) {
      val id = participantRef.domains.id_of(domainAlias)
      fetch_domain_time(id, timeout)
    }

  @Help.Summary("Fetch the current time from the given domain", FeatureFlag.Testing)
  def fetch_domain_time(
      domainId: DomainId,
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ledgerCommand,
  ): CantonTimestamp =
    check(FeatureFlag.Testing) {
      consoleEnvironment.run {
        adminCommand(
          DomainTimeCommands.FetchTime(
            domainId.some,
            NonNegativeFiniteDuration.Zero,
            timeout,
          )
        )
      }.timestamp
    }

  @Help.Summary("Fetch the current time from all connected domains", FeatureFlag.Testing)
  def fetch_domain_times(
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ledgerCommand
  ): Unit =
    check(FeatureFlag.Testing) {
      participantRef.domains.list_connected().foreach { item =>
        fetch_domain_time(item.domainId, timeout).discard[CantonTimestamp]
      }
    }

  @Help.Summary("Await for the given time to be reached on the given domain", FeatureFlag.Testing)
  def await_domain_time(
      domainAlias: DomainAlias,
      time: CantonTimestamp,
      timeout: NonNegativeDuration,
  ): Unit =
    check(FeatureFlag.Testing) {
      val id = participantRef.domains.id_of(domainAlias)
      await_domain_time(id, time, timeout)
    }

  @Help.Summary("Await for the given time to be reached on the given domain", FeatureFlag.Testing)
  def await_domain_time(
      domainId: DomainId,
      time: CantonTimestamp,
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ledgerCommand,
  ): Unit =
    check(FeatureFlag.Testing) {
      consoleEnvironment.run {
        adminCommand(
          DomainTimeCommands.AwaitTime(
            domainId.some,
            time,
            timeout,
          )
        )
      }
    }
}

class LocalParticipantTestingGroup(
    participantRef: ParticipantReferenceCommon with BaseInspection[ParticipantNodeCommon],
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends ParticipantTestingGroup(participantRef, consoleEnvironment, loggerFactory)
    with FeatureFlagFilter
    with NoTracing {

  protected def defaultLimit: PositiveInt =
    consoleEnvironment.environment.config.parameters.console.defaultLimit

  import participantRef.*
  @Help.Summary("Lookup contracts in the Private Contract Store", FeatureFlag.Testing)
  @Help.Description("""Get raw access to the PCS of the given domain sync controller.
  The filter commands will check if the target value ``contains`` the given string.
  The arguments can be started with ``^`` such that ``startsWith`` is used for comparison or ``!`` to use ``equals``.
  The ``activeSet`` argument allows to restrict the search to the active contract set.
  """)
  def pcs_search(
      domainAlias: DomainAlias,
      // filter by id (which is txId::discriminator, so can be used to look for both)
      filterId: String = "",
      filterPackage: String = "",
      filterTemplate: String = "",
      // only include active contracts
      activeSet: Boolean = false,
      limit: PositiveInt = defaultLimit,
  ): List[(Boolean, SerializableContract)] = {
    def toOpt(str: String) = OptionUtil.emptyStringAsNone(str)

    val pcs = state_inspection
      .findContracts(
        domainAlias,
        toOpt(filterId),
        toOpt(filterPackage),
        toOpt(filterTemplate),
        limit.value,
      )
    if (activeSet) pcs.filter { case (isActive, _) => isActive }
    else pcs
  }

  @Help.Summary("Lookup of active contracts", FeatureFlag.Testing)
  def acs_search(
      domainAlias: DomainAlias,
      // filter by id (which is txId::discriminator, so can be used to look for both)
      filterId: String = "",
      filterPackage: String = "",
      filterTemplate: String = "",
      filterStakeholder: Option[PartyId] = None,
      limit: PositiveInt = defaultLimit,
  ): List[SerializableContract] = {
    val predicate = (c: SerializableContract) =>
      filterStakeholder.forall(s => c.metadata.stakeholders.contains(s.toLf))

    check(FeatureFlag.Testing) {
      pcs_search(domainAlias, filterId, filterPackage, filterTemplate, activeSet = true, limit)
        .map(_._2)
        .filter(predicate)
    }
  }

  @Help.Summary("Lookup of events", FeatureFlag.Testing)
  @Help.Description(
    """Show the event logs. To select only events from a particular domain, use the domain alias.
       Leave the domain empty to search the combined event log containing the events of all domains.
       Note that if the domain is left blank, the values of `from` and `to` cannot be set.
       This is because the combined event log isn't guaranteed to have increasing timestamps.
    """
  )
  def event_search(
      domain: Option[DomainAlias] = None,
      from: Option[Instant] = None,
      to: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[(String, TimestampedEvent)] = {
    check(FeatureFlag.Testing) {
      if (domain.isEmpty && (from.isDefined || to.isDefined)) {
        logger.error(
          s"You are not allowed to set values for 'from' and 'to' if searching the combined event log " +
            s"(you are searching the combined event log because you left the domain blank)."
        )
        throw new CommandFailure()
      } else {
        stateInspection.findEvents(
          domain,
          from.map(timestampFromInstant),
          to.map(timestampFromInstant),
          Some(limit.value),
        )
      }
    }
  }

  @Help.Summary("Lookup of accepted transactions", FeatureFlag.Testing)
  @Help.Description("""Show the accepted transactions as they appear in the event logs.
       To select only transactions from a particular domain, use the domain alias.
       Leave the domain empty to search the combined event log containing the events of all domains.
       Note that if the domain is left blank, the values of `from` and `to` cannot be set.
       This is because the combined event log isn't guaranteed to have increasing timestamps.
    """)
  def transaction_search(
      domain: Option[DomainAlias] = None,
      from: Option[Instant] = None,
      to: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[(String, LfCommittedTransaction)] =
    check(FeatureFlag.Testing) {
      if (domain.isEmpty && (from.isDefined || to.isDefined)) {
        logger.error(
          s"You are not allowed to set values for 'from' and 'to' if searching the combined event log " +
            s"(you are searching the combined event log because you left the domain blank)."
        )
        throw new CommandFailure()
      } else {
        stateInspection.findAcceptedTransactions(
          domain,
          from.map(timestampFromInstant),
          to.map(timestampFromInstant),
          Some(limit.value),
        )
      }
    }

  @Help.Summary("Retrieve all sequencer messages", FeatureFlag.Testing)
  @Help.Description("""Optionally allows filtering for sequencer from a certain time span (inclusive on both ends) and
      |limiting the number of displayed messages. The returned messages will be ordered on most domain ledger implementations
      |if a time span is given.
      |
      |Fails if the participant has never connected to the domain.""")
  def sequencer_messages(
      domain: DomainAlias,
      from: Option[Instant] = None,
      to: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[PossiblyIgnoredProtocolEvent] =
    state_inspection.findMessages(domain, from, to, Some(limit.value))

  @Help.Summary(
    "Return the sync crypto api provider, which provides access to all cryptographic methods",
    FeatureFlag.Testing,
  )
  def crypto_api(): SyncCryptoApiProvider = check(FeatureFlag.Testing) {
    access(node => node.sync.syncCrypto)
  }

  @Help.Summary(
    "The latest timestamp before or at the given one for which no commitment is outstanding",
    FeatureFlag.Testing,
  )
  @Help.Description(
    """The latest timestamp before or at the given one for which no commitment is outstanding.
      |Note that this doesn't imply that pruning is possible at this timestamp, as the system might require some
      |additional data for crash recovery. Thus, this is useful for testing commitments; use the commands in the pruning
      |group for pruning.
      |Additionally, the result needn't fall on a "commitment tick" as specified by the reconciliation interval."""
  )
  def find_clean_commitments_timestamp(
      domain: DomainAlias,
      beforeOrAt: CantonTimestamp = CantonTimestamp.now(),
  ): Option[CantonTimestamp] =
    state_inspection.noOutstandingCommitmentsTs(domain, beforeOrAt)

  @Help.Summary(
    "Obtain access to the state inspection interface. Use at your own risk.",
    FeatureFlag.Testing,
  )
  @Help.Description(
    """The state inspection methods can fatally and permanently corrupt the state of a participant.
      |The API is subject to change in any way."""
  )
  def state_inspection: SyncStateInspection = check(FeatureFlag.Testing) { stateInspection }

  private def stateInspection: SyncStateInspection = access(node => node.sync.stateInspection)

}

class ParticipantPruningAdministrationGroup(
    runner: LedgerApiCommandRunner & AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends PruningSchedulerAdministration(
      runner,
      consoleEnvironment,
      new PruningSchedulerCommands[PruningServiceStub](
        PruningServiceGrpc.stub,
        _.setSchedule(_),
        _.clearSchedule(_),
        _.setCron(_),
        _.setMaxDuration(_),
        _.setRetention(_),
        _.getSchedule(_),
      ),
      loggerFactory,
    )
    with FeatureFlagFilter
    with Helpful {

  import runner.*

  @Help.Summary("Prune the ledger up to the specified offset inclusively.")
  @Help.Description(
    """Prunes the participant ledger up to the specified offset inclusively returning ``Unit`` if the ledger has been
      |successfully pruned.
      |Note that upon successful pruning, subsequent attempts to read transactions via ``ledger_api.transactions.flat`` or
      |``ledger_api.transactions.trees`` or command completions via ``ledger_api.completions.list`` by specifying a begin offset
      |lower than the returned pruning offset will result in a ``NOT_FOUND`` error.
      |In the Enterprise Edition, ``prune`` performs a "full prune" freeing up significantly more space and also
      |performs additional safety checks returning a ``NOT_FOUND`` error if ``pruneUpTo`` is higher than the
      |offset returned by ``find_safe_offset`` on any domain with events preceding the pruning offset."""
  )
  def prune(pruneUpTo: LedgerOffset): Unit =
    consoleEnvironment.run(
      ledgerApiCommand(LedgerApiCommands.ParticipantPruningService.Prune(pruneUpTo))
    )

  @Help.Summary(
    "Return the highest participant ledger offset whose record time is before or at the given one (if any) at which pruning is safely possible",
    FeatureFlag.Preview,
  )
  def find_safe_offset(beforeOrAt: Instant = Instant.now()): Option[ParticipantOffset] = {
    check(FeatureFlag.Preview) {
      val ledgerEnd = consoleEnvironment.run(
        ledgerApiCommand(LedgerApiCommands.TransactionService.GetLedgerEnd())
      )
      consoleEnvironment
        .run(
          adminCommand(
            ParticipantAdminCommands.Pruning.GetSafePruningOffsetCommand(beforeOrAt, ledgerEnd)
          )
        )
        .map(ledgerOffset => ApiConversions.toV2(ledgerOffset))
    }
  }

  @Help.Summary(
    "Prune only internal ledger state up to the specified offset inclusively.",
    FeatureFlag.Preview,
  )
  @Help.Description(
    """Special-purpose variant of the ``prune`` command only available in the Enterprise Edition that prunes only partial,
      |internal participant ledger state freeing up space not needed for serving ``ledger_api.transactions``
      |and ``ledger_api.completions`` requests. In conjunction with ``prune``, ``prune_internally`` enables pruning
      |internal ledger state more aggressively than externally observable data via the ledger api. In most use cases
      |``prune`` should be used instead. Unlike ``prune``, ``prune_internally`` has no visible effect on the Ledger API.
      |The command returns ``Unit`` if the ledger has been successfully pruned or an error if the timestamp
      |performs additional safety checks returning a ``NOT_FOUND`` error if ``pruneUpTo`` is higher than the
      |offset returned by ``find_safe_offset`` on any domain with events preceding the pruning offset."""
  )
  // Consider adding an "Enterprise" annotation if we end up having more enterprise-only commands than this lone enterprise command.
  def prune_internally(pruneUpTo: LedgerOffset): Unit =
    check(FeatureFlag.Preview) {
      consoleEnvironment.run(
        adminCommand(ParticipantAdminCommands.Pruning.PruneInternallyCommand(pruneUpTo))
      )
    }

  @Help.Summary(
    "Activate automatic pruning according to the specified schedule with participant-specific options."
  )
  @Help.Description(
    """Refer to the ``set_schedule`` description for information about the "cron", "max_duration", and "retention"
      |parameters. Setting the "prune_internally_only" flag causes pruning to only remove internal state as described in
      |more detail in the ``prune_internally`` command description.
  """
  )
  def set_participant_schedule(
      cron: String,
      maxDuration: config.PositiveDurationSeconds,
      retention: config.PositiveDurationSeconds,
      pruneInternallyOnly: Boolean = false,
  ): Unit =
    check(FeatureFlag.Preview) {
      consoleEnvironment.run(
        runner.adminCommand(
          SetParticipantScheduleCommand(
            cron,
            maxDuration,
            retention,
            pruneInternallyOnly,
          )
        )
      )
    }

  @Help.Summary("Inspect the automatic, participant-specific pruning schedule.")
  @Help.Description(
    """The schedule consists of a "cron" expression and "max_duration" and "retention" durations as described in the
      |``get_schedule`` command description. Additionally "prune_internally" indicates if the schedule mandates
      |pruning of internal state.
  """
  )
  def get_participant_schedule(): Option[ParticipantPruningSchedule] =
    consoleEnvironment.run(
      runner.adminCommand(GetParticipantScheduleCommand())
    )

  @Help.Summary(
    "Identify the participant ledger offset to prune up to based on the specified timestamp."
  )
  @Help.Description(
    """Return the largest participant ledger offset that has been processed before or at the specified timestamp.
      |The time is measured on the participant's local clock at some point while the participant has processed the
      |the event. Returns ``None`` if no such offset exists.
    """
  )
  def get_offset_by_time(upToInclusive: Instant): Option[LedgerOffset] =
    consoleEnvironment.run(
      adminCommand(
        ParticipantAdminCommands.Inspection.LookupOffsetByTime(
          ProtoConverter.InstantConverter.toProtoPrimitive(upToInclusive)
        )
      )
    ) match {
      case "" => None
      case offset => Some(LedgerOffset(LedgerOffset.Value.Absolute(offset)))
    }

  @Help.Summary("Identify the participant ledger offset to prune up to.", FeatureFlag.Preview)
  @Help.Description(
    """Return the participant ledger offset that corresponds to pruning "n" number of transactions
      |from the beginning of the ledger. Errors if the ledger holds less than "n" transactions. Specifying "n" of 1
      |returns the offset of the first transaction (if the ledger is non-empty).
    """
  )
  def locate_offset(n: Long): LedgerOffset =
    check(FeatureFlag.Preview) {
      val rawOffset = consoleEnvironment.run(
        adminCommand(ParticipantAdminCommands.Inspection.LookupOffsetByIndex(n))
      )
      LedgerOffset(LedgerOffset.Value.Absolute(rawOffset))
    }

}

class LocalCommitmentsAdministrationGroup(
    runner: AdminCommandRunner with BaseInspection[ParticipantNodeCommon],
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with Helpful
    with NoTracing {

  import runner.*

  @Help.Summary(
    "Lookup ACS commitments received from other participants as part of the reconciliation protocol"
  )
  @Help.Description("""The arguments are:
       - domain: the alias of the domain
       - start: lowest time exclusive
       - end: highest time inclusive
       - counterParticipant: optionally filter by counter participant
      """)
  def received(
      domain: DomainAlias,
      start: Instant,
      end: Instant,
      counterParticipant: Option[ParticipantId] = None,
  ): Iterable[SignedProtocolMessage[AcsCommitment]] = {
    access(node =>
      node.sync.stateInspection
        .findReceivedCommitments(
          domain,
          timestampFromInstant(start),
          timestampFromInstant(end),
          counterParticipant,
        )
    )
  }

  @Help.Summary("Lookup ACS commitments locally computed as part of the reconciliation protocol")
  def computed(
      domain: DomainAlias,
      start: Instant,
      end: Instant,
      counterParticipant: Option[ParticipantId] = None,
  ): Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)] =
    access { node =>
      node.sync.stateInspection.findComputedCommitments(
        domain,
        timestampFromInstant(start),
        timestampFromInstant(end),
        counterParticipant,
      )
    }

  def outstanding(
      domain: DomainAlias,
      start: Instant,
      end: Instant,
      counterParticipant: Option[ParticipantId] = None,
  ): Iterable[(CommitmentPeriod, ParticipantId)] =
    access { node =>
      node.sync.stateInspection.outstandingCommitments(
        domain,
        timestampFromInstant(start),
        timestampFromInstant(end),
        counterParticipant,
      )
    }

}

class ParticipantReplicationAdministrationGroup(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
) extends Helpful {

  @Help.Summary("Set the participant replica to passive")
  @Help.Description(
    "Trigger a graceful fail-over from this active replica to another passive replica."
  )
  def set_passive(): Unit = {
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.Replication.SetPassiveCommand()
      )
    }
  }

}

/** Administration commands supported by a participant.
  */
trait ParticipantAdministration extends FeatureFlagFilter {
  this: AdminCommandRunner
    with LedgerApiCommandRunner
    with LedgerApiAdministration
    with NamedLogging =>

  import ConsoleEnvironment.Implicits.*
  implicit protected val consoleEnvironment: ConsoleEnvironment

  private val runner = this

  def id: ParticipantId

  protected def waitPackagesVetted(
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.bounded
  ): Unit

  protected def participantIsActiveOnDomain(
      domainId: DomainId,
      participantId: ParticipantId,
  ): Boolean

  @Help.Summary("Manage DAR packages")
  @Help.Group("DAR Management")
  object dars extends Helpful {
    @Help.Summary(
      "Remove a DAR from the participant",
      FeatureFlag.Preview,
    )
    @Help.Description(
      """Can be used to remove a DAR from the participant, if the following conditions are satisfied:
        |1. The main package of the DAR must be unused -- there should be no active contract from this package
        |
        |2. All package dependencies of the DAR should either be unused or contained in another of the participant node's uploaded DARs. Canton uses this restriction to ensure that the package dependencies of the DAR don't become "stranded" if they're in use.
        |
        |3. The main package of the dar should not be vetted. If it is vetted, Canton will try to automatically
        |   revoke the vetting for the main package of the DAR, but this automatic vetting revocation will only succeed if the
        |   main package vetting originates from a standard ``dars.upload``. Even if the automatic revocation fails, you can
        |   always manually revoke the package vetting.
        |
        |If synchronizeVetting is true (default), then the command will block until the participant has observed the vetting transactions to be registered with the domain.
        |"""
    )
    def remove(darHash: String, synchronizeVetting: Boolean = true): Unit = {
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.Package.RemoveDar(darHash))
      })
      if (synchronizeVetting) {
        packages.synchronize_vetting()
      }
    }

    @Help.Summary("List installed DAR files")
    @Help.Description("""List DARs installed on this participant
      |The arguments are:
      |  filterName: filter by name (source description)
      |  limit: Limit number of results (default none)
      """)
    def list(limit: PositiveInt = defaultLimit, filterName: String = ""): Seq[v0.DarDescription] =
      consoleEnvironment
        .run {
          adminCommand(ParticipantAdminCommands.Package.ListDars(limit))
        }
        .filter(_.name.startsWith(filterName))

    @Help.Summary("List contents of DAR files")
    def list_contents(hash: String): DarMetadata = consoleEnvironment.run {
      adminCommand(
        ParticipantAdminCommands.Package.ListDarContents(hash)
      )
    }

    @Help.Summary("Upload a Dar to Canton")
    @Help.Description("""Daml code is normally shipped as a Dar archive and must explicitly be uploaded to a participant.
        |A Dar is a collection of LF-packages, the native binary representation of Daml smart contracts.
        |In order to use Daml templates on a participant, the Dar must first be uploaded and then
        |vetted by the participant. Vetting will ensure that other participants can check whether they
        |can actually send a transaction referring to a particular Daml package and participant.
        |Vetting is done by registering a VettedPackages topology transaction with the topology manager.
        |By default, vetting happens automatically and this command waits for
        |the vetting transaction to be successfully registered on all connected domains.
        |This is the safe default setting minimizing race conditions.
        |
        |If vetAllPackages is true (default), the packages will all be vetted on all domains the participant is registered.
        |If synchronizeVetting is true (default), then the command will block until the participant has observed the vetting transactions to be registered with the domain.
        |
        |Note that synchronize vetting might block on permissioned domains that do not just allow participants to update the topology state.
        |In such cases, synchronizeVetting should be turned off.
        |Synchronize vetting can be invoked manually using $participant.package.synchronize_vettings()
        |""")
    def upload(
        path: String,
        vetAllPackages: Boolean = true,
        synchronizeVetting: Boolean = true,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): String = {
      val res = consoleEnvironment.runE {
        for {
          hash <- ParticipantCommands.dars
            .upload(runner, path, vetAllPackages, synchronizeVetting, logger)
            .toEither
        } yield hash
      }
      if (synchronizeVetting && vetAllPackages) {
        packages.synchronize_vetting()
        synchronize.foreach { timeout =>
          ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
        }
      }
      res
    }

    @Help.Summary("Downloads the DAR file with the given hash to the given directory")
    def download(darHash: String, directory: String): Unit = {
      val _ = consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.Package.GetDar(Some(darHash), Some(directory), logger)
        )
      }
    }

    @Help.Summary("Change DAR vetting status")
    @Help.Group("Vetting")
    object vetting extends Helpful {
      @Help.Summary(
        "Vet all packages contained in the DAR archive identified by the provided DAR hash."
      )
      def enable(darHash: String, synchronize: Boolean = true): Unit =
        check(FeatureFlag.Preview)(consoleEnvironment.run {
          adminCommand(ParticipantAdminCommands.Package.VetDar(darHash, synchronize))
        })

      @Help.Summary("""Revoke vetting for all packages contained in the DAR archive
          |identified by the provided DAR hash.""")
      @Help.Description("""This command succeeds if the vetting command used to vet the DAR's packages
          |was symmetric and resulted in a single vetting topology transaction for all the packages in the DAR.
          |This command is potentially dangerous and misuse
          |can lead the participant to fail in processing transactions""")
      def disable(darHash: String): Unit =
        check(FeatureFlag.Preview)(consoleEnvironment.run {
          adminCommand(ParticipantAdminCommands.Package.UnvetDar(darHash))
        })
    }

  }

  @Help.Summary("Manage raw Daml-LF packages")
  @Help.Group("Package Management")
  object packages extends Helpful {

    @Help.Summary("List packages stored on the participant")
    @Help.Description("""Supported arguments:
        limit - Limit on the number of packages returned (defaults to canton.parameters.console.default-limit)
        """)
    def list(limit: PositiveInt = defaultLimit): Seq[v0.PackageDescription] =
      consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.Package.List(limit))
      }

    @Help.Summary("List package contents")
    def list_contents(packageId: String): Seq[v0.ModuleDescription] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.Package.ListContents(packageId))
    }

    @Help.Summary("Find packages that contain a module with the given name")
    def find(
        moduleName: String,
        limitPackages: PositiveInt = defaultLimit,
    ): Seq[v0.PackageDescription] = consoleEnvironment.runE {
      val packageC = adminCommand(ParticipantAdminCommands.Package.List(limitPackages)).toEither
      val matchingC = packageC
        .flatMap { packages =>
          packages.traverse(x =>
            adminCommand(ParticipantAdminCommands.Package.ListContents(x.packageId)).toEither.map(
              r => (x, r)
            )
          )
        }
      matchingC.map(_.filter { case (_, content) =>
        content.map(_.name).contains(moduleName)
      }.map(_._1))
    }

    @Help.Summary(
      "Remove the package from Canton's package store.",
      FeatureFlag.Preview,
    )
    @Help.Description(
      """The standard operation of this command checks that a package is unused and unvetted, and if so
        |removes the package. The force flag can be used to disable the checks, but do not use the force flag unless
        |you're certain you know what you're doing. """
    )
    def remove(packageId: String, force: Boolean = false): Unit = {
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.Package.RemovePackage(packageId, force))
      })
    }

    @Help.Summary(
      "Ensure that all vetting transactions issued by this participant have been observed by all configured participants"
    )
    @Help.Description("""Sometimes, when scripting tests and demos, a dar or package is uploaded and we need to ensure
        |that commands are only submitted once the package vetting has been observed by some other connected participant
        |known to the console. This command can be used in such cases.""")
    def synchronize_vetting(
        timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.bounded
    ): Unit = {

      // ensure that the ledger api server has seen all packages
      try {
        AdminCommandRunner.retryUntilTrue(timeout) {
          val canton = packages.list().map(_.packageId).toSet
          val maxPackages = PositiveInt.tryCreate(1000)
          val lApi = consoleEnvironment
            .run {
              ledgerApiCommand(
                LedgerApiCommands.PackageService.ListKnownPackages(maxPackages)
              )
            }
            .map(_.packageId)
            .toSet
          // don't synchronise anymore in a big production system (as we only need this truly for testing)
          (lApi.size >= maxPackages.value) || (canton -- lApi).isEmpty
        }
      } catch {
        case _: TimeoutException =>
          logger.error(
            show"Participant $id ledger Api server has still a different set of packages than the sync server"
          )
      }

      waitPackagesVetted(timeout)
    }
  }

  @Help.Summary("Manage domain connections")
  @Help.Group("Domains")
  object domains extends Helpful {

    @Help.Summary("Returns the id of the given domain alias")
    def id_of(domainAlias: DomainAlias): DomainId = {
      consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.DomainConnectivity.GetDomainId(domainAlias))
      }
    }

    @Help.Summary(
      "Test whether a participant is connected to and permissioned on a domain."
    )
    @Help.Description(
      """Yields false, if the domain is not connected or not healthy.
        |Yields false, if the domain is configured in the Canton configuration and
        |the participant is not active from the perspective of the domain."""
    )
    def active(domainAlias: DomainAlias): Boolean = {
      list_connected().exists(r => {
        val domainReferenceO = consoleEnvironment.nodes.all
          .collectFirst {
            case d: DomainAdministration
                if d.health.status.successOption.exists(_.uid == r.domainId.unwrap) =>
              d
          }

        r.domainAlias == domainAlias &&
        r.healthy &&
        participantIsActiveOnDomain(r.domainId, id) &&
        domainReferenceO.forall(_.participants.active(id))
      })
    }

    @Help.Summary(
      "Test whether a participant is connected to and permissioned on a domain reference, both from the perspective of the participant and the domain."
    )
    @Help.Description(
      "Yields false, if the domain has not been initialized, is not connected or is not healthy."
    )
    def active(reference: DomainAdministration): Boolean = {
      val domainUidO = reference.health.status.successOption.map(_.uid)
      list_connected()
        .exists(r =>
          domainUidO.contains(r.domainId.unwrap) &&
            r.healthy &&
            participantIsActiveOnDomain(r.domainId, id) &&
            reference.participants.active(id)
        )
    }

    @Help.Summary(
      "Test whether a participant is connected to a domain reference"
    )
    def is_connected(reference: DomainAdministration): Boolean =
      list_connected().exists(_.domainId == reference.id)

    @Help.Summary(
      "Test whether a participant is connected to a domain reference"
    )
    def is_connected(domainId: DomainId): Boolean =
      list_connected().exists(_.domainId == domainId)

    private def confirm_agreement(domainAlias: DomainAlias): Unit = {

      val response = get_agreement(domainAlias)

      val autoApprove =
        sys.env.getOrElse("CANTON_AUTO_APPROVE_AGREEMENTS", "no").toLowerCase == "yes"
      response.foreach {
        case (agreement, accepted) if !accepted =>
          if (autoApprove) {
            accept_agreement(domainAlias.unwrap, agreement.id)
          } else {
            println(s"Service Agreement for `$domainAlias`:")
            println(agreement.text)
            println("Do you accept the license? yes/no")
            print("> ")
            val answer = Option(scala.io.StdIn.readLine())
            if (answer.exists(_.toLowerCase == "yes"))
              accept_agreement(domainAlias.unwrap, agreement.id)
          }
        case _ => () // Don't do anything if the license has already been accepted
      }
    }

    @Help.Summary(
      "Macro to connect a participant to a locally configured domain given by reference"
    )
    @Help.Description("""
        The arguments are:
          domain - A local domain or sequencer reference
          manualConnect - Whether this connection should be handled manually and also excluded from automatic re-connect.
          alias - The name you will be using to refer to this domain. Can not be changed anymore.
          certificatesPath - Path to TLS certificate files to use as a trust anchor.
          priority - The priority of the domain. The higher the more likely a domain will be used.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def connect_local(
        domain: InstanceReferenceWithSequencerConnection,
        manualConnect: Boolean = false,
        alias: Option[DomainAlias] = None,
        maxRetryDelayMillis: Option[Long] = None,
        priority: Int = 0,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Unit = {
      val config = ParticipantCommands.domains.referenceToConfig(
        NonEmpty.mk(Seq, SequencerAlias.Default -> domain).toMap,
        manualConnect,
        alias,
        maxRetryDelayMillis.map(NonNegativeFiniteDuration.tryOfMillis),
        priority,
      )
      connectFromConfig(config, synchronize)
    }

    def connect_local_bft(
        domain: NonEmpty[Map[SequencerAlias, InstanceReferenceWithSequencerConnection]],
        manualConnect: Boolean = false,
        alias: Option[DomainAlias] = None,
        maxRetryDelayMillis: Option[Long] = None,
        priority: Int = 0,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        sequencerTrustThreshold: PositiveInt = PositiveInt.tryCreate(1),
    ): Unit = {
      val config = ParticipantCommands.domains.referenceToConfig(
        domain,
        manualConnect,
        alias,
        maxRetryDelayMillis.map(NonNegativeFiniteDuration.tryOfMillis),
        priority,
        sequencerTrustThreshold,
      )
      connectFromConfig(config, synchronize)
    }

    @Help.Summary("Macro to connect a participant to a domain given by connection")
    @Help.Description("""This variant of connect expects a domain connection config.
        |Otherwise the behaviour is equivalent to the connect command with explicit
        |arguments. If the domain is already configured, the domain connection
        |will be attempted. If however the domain is offline, the command will fail.
        |Generally, this macro should only be used to setup a new domain. However, for
        |convenience, we support idempotent invocations where subsequent calls just ensure
        |that the participant reconnects to the domain.
        |""")
    def connect(
        config: DomainConnectionConfig
    ): Unit = {
      connectFromConfig(config, None)
    }

    @Help.Summary("Macro to connect a participant to a domain given by instance")
    @Help.Description("""This variant of connect expects an instance with a sequencer connection.
        |Otherwise the behaviour is equivalent to the connect command with explicit
        |arguments. If the domain is already configured, the domain connection
        |will be attempted. If however the domain is offline, the command will fail.
        |Generally, this macro should only be used to setup a new domain. However, for
        |convenience, we support idempotent invocations where subsequent calls just ensure
        |that the participant reconnects to the domain.
        |""")
    def connect(
        instance: InstanceReferenceWithSequencerConnection,
        domainAlias: DomainAlias,
    ): Unit =
      connect(
        DomainConnectionConfig(
          domainAlias,
          SequencerConnections.single(instance.sequencerConnection),
        )
      )

    private def connectFromConfig(
        config: DomainConnectionConfig,
        synchronize: Option[NonNegativeDuration],
    ): Unit = {
      val current = this.config(config.domain)
      // if the config did not change, we'll just treat this as idempotent, otherwise, we'll use register to fail
      if (current.isEmpty) {
        // architecture-handbook-entry-begin: OnboardParticipantConnect
        // register the domain configuration
        register(config.copy(manualConnect = true))
        if (!config.manualConnect) {
          // fetch and confirm domain agreement
          if (config.sequencerConnections.nonBftSetup) { // agreement is removed with the introduction of BFT domain.
            confirm_agreement(config.domain.unwrap)
          }
          reconnect(config.domain.unwrap, retry = false).discard
          // now update the domain settings to auto-connect
          modify(config.domain.unwrap, _.copy(manualConnect = false))
        }
        // architecture-handbook-entry-end: OnboardParticipantConnect
      } else if (!config.manualConnect) {
        val _ = reconnect(config.domain, retry = false)
        modify(config.domain.unwrap, _.copy(manualConnect = false))
      }
      synchronize.foreach { timeout =>
        ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
      }
    }

    @Help.Summary("Macro to connect a participant to a domain given by connection")
    @Help.Description("""The connect macro performs a series of commands in order to connect this participant to a domain.
        |First, `register` will be invoked with the given arguments, but first registered
        |with manualConnect = true. If you already set manualConnect = true, then nothing else
        |will happen and you will have to do the remaining steps yourselves.
        |Otherwise, if the domain requires an agreement, it is fetched and presented to the user for evaluation.
        |If the user is fine with it, the agreement is confirmed. If you want to auto-confirm,
        |then set the environment variable CANTON_AUTO_APPROVE_AGREEMENTS=yes.
        |Finally, the command will invoke `reconnect` to startup the connection.
        |If the reconnect succeeded, the registered configuration will be updated
        |with manualStart = true. If anything fails, the domain will remain registered with `manualConnect = true` and
        |you will have to perform these steps manually.
        The arguments are:
          domainAlias - The name you will be using to refer to this domain. Can not be changed anymore.
          connection - The connection string to connect to this domain. I.e. https://url:port
          manualConnect - Whether this connection should be handled manually and also excluded from automatic re-connect.
          domainId - Optionally the domainId you expect to see on this domain.
          certificatesPath - Path to TLS certificate files to use as a trust anchor.
          priority - The priority of the domain. The higher the more likely a domain will be used.
          timeTrackerConfig - The configuration for the domain time tracker.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def connect(
        domainAlias: DomainAlias,
        connection: String,
        manualConnect: Boolean = false,
        domainId: Option[DomainId] = None,
        certificatesPath: String = "",
        priority: Int = 0,
        timeTrackerConfig: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): DomainConnectionConfig = {
      val config = ParticipantCommands.domains.toConfig(
        domainAlias,
        connection,
        manualConnect,
        domainId,
        certificatesPath,
        priority,
        timeTrackerConfig = timeTrackerConfig,
      )
      connectFromConfig(config, synchronize)
      config
    }

    @Help.Summary(
      "Macro to connect a participant to a domain that supports connecting via many endpoints"
    )
    @Help.Description("""Domains can provide many endpoints to connect to for availability and performance benefits.
        This version of connect allows specifying multiple endpoints for a single domain connection:
           connect_multi("mydomain", Seq(sequencer1, sequencer2))
           or:
           connect_multi("mydomain", Seq("https://host1.mydomain.net", "https://host2.mydomain.net", "https://host3.mydomain.net"))

        To create a more advanced connection config use domains.toConfig with a single host,
        |then use config.addConnection to add additional connections before connecting:
           config = myparticipaint.domains.toConfig("mydomain", "https://host1.mydomain.net", ...otherArguments)
           config = config.addConnection("https://host2.mydomain.net", "https://host3.mydomain.net")
           myparticipant.domains.connect(config)

        The arguments are:
          domainAlias - The name you will be using to refer to this domain. Can not be changed anymore.
          connections - The sequencer connection definitions (can be an URL) to connect to this domain. I.e. https://url:port
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def connect_multi(
        domainAlias: DomainAlias,
        connections: Seq[SequencerConnection],
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): DomainConnectionConfig = {
      val sequencerConnection =
        SequencerConnection.merge(connections).getOrElse(sys.error("Invalid sequencer connection"))
      val sequencerConnections =
        SequencerConnections.single(sequencerConnection)
      val config = DomainConnectionConfig(
        domainAlias,
        sequencerConnections,
      )
      connectFromConfig(config, synchronize)
      config
    }

    @Help.Summary("Reconnect this participant to the given domain")
    @Help.Description("""Idempotent attempts to re-establish a connection to a certain domain.
        |If retry is set to false, the command will throw an exception if unsuccessful.
        |If retry is set to true, the command will terminate after the first attempt with the result,
        |but the server will keep on retrying to connect to the domain.
        |
        The arguments are:
          domainAlias - The name you will be using to refer to this domain. Can not be changed anymore.
          retry - Whether the reconnect should keep on retrying until it succeeded or abort noisly if the connection attempt fails.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def reconnect(
        domainAlias: DomainAlias,
        retry: Boolean = true,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Boolean = {
      val ret = consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.DomainConnectivity.ConnectDomain(domainAlias, retry))
      }
      if (ret) {
        synchronize.foreach { timeout =>
          ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
        }
      }
      ret
    }

    @Help.Summary("Reconnect this participant to the given local domain")
    @Help.Description("""Idempotent attempts to re-establish a connection to the given local domain.
        |Same behaviour as generic reconnect.

        The arguments are:
          ref - The domain reference to connect to
          retry - Whether the reconnect should keep on retrying until it succeeded or abort noisly if the connection attempt fails.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def reconnect_local(
        ref: InstanceReferenceWithSequencerConnection
    ): Boolean = reconnect(ref.name)

    @Help.Summary("Reconnect this participant to the given local domain")
    @Help.Description("""Idempotent attempts to re-establish a connection to the given local domain.
        |Same behaviour as generic reconnect.

        The arguments are:
          domainAlias - The domain alias to connect to
          retry - Whether the reconnect should keep on retrying until it succeeded or abort noisly if the connection attempt fails.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def reconnect_local(
        domainAlias: DomainAlias,
        retry: Boolean = true,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Boolean = reconnect(domainAlias, retry, synchronize)

    @Help.Summary("Reconnect this participant to all domains which are not marked as manual start")
    @Help.Description("""
      The arguments are:
          ignoreFailures - If set to true (default), we'll attempt to connect to all, ignoring any failure
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
    """)
    def reconnect_all(
        ignoreFailures: Boolean = true,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Unit = {
      consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.DomainConnectivity.ReconnectDomains(ignoreFailures)
        )
      }
      synchronize.foreach { timeout =>
        ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
      }
    }

    @Help.Summary("Disconnect this participant from the given domain")
    def disconnect(domainAlias: DomainAlias): Unit = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.DomainConnectivity.DisconnectDomain(domainAlias))
    }

    @Help.Summary("Disconnect this participant from all connected domains")
    def disconnect_all(): Unit = {
      list_connected().foreach { connected =>
        disconnect(connected.domainAlias)
      }
    }

    @Help.Summary("Disconnect this participant from the given local domain")
    def disconnect_local(domain: DomainReference): Unit = disconnect_local(domain.name)

    @Help.Summary("Disconnect this participant from the given local domain")
    def disconnect_local(domain: DomainAlias): Unit = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.DomainConnectivity.DisconnectDomain(domain))
    }

    @Help.Summary("List the connected domains of this participant")
    def list_connected(): Seq[ListConnectedDomainsResult] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.DomainConnectivity.ListConnectedDomains())
    }

    @Help.Summary("List the configured domains of this participant")
    def list_registered(): Seq[(DomainConnectionConfig, Boolean)] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.DomainConnectivity.ListConfiguredDomains)
    }

    @Help.Summary("Returns true if a domain is registered using the given alias")
    def is_registered(domain: DomainAlias): Boolean =
      config(domain).nonEmpty

    @Help.Summary("Returns the current configuration of a given domain")
    def config(domain: DomainAlias): Option[DomainConnectionConfig] =
      list_registered().map(_._1).find(_.domain == domain)

    @Help.Summary("Register new domain connection")
    @Help.Description("""When connecting to a domain, we need to register the domain connection and eventually
        |accept the terms of service of the domain before we can connect. The registration process is therefore
        |a subset of the operation. Therefore, register is equivalent to connect if the domain does not require
        |a service agreement. However, you would usually call register only in advanced scripts.""")
    def register(config: DomainConnectionConfig): Unit = {
      consoleEnvironment.run {
        ParticipantCommands.domains.register(runner, config)
      }
    }

    @Help.Summary("Modify existing domain connection")
    def modify(
        domain: DomainAlias,
        modifier: DomainConnectionConfig => DomainConnectionConfig,
    ): Unit = {
      consoleEnvironment.runE {
        for {
          configured <- adminCommand(
            ParticipantAdminCommands.DomainConnectivity.ListConfiguredDomains
          ).toEither
          cfg <- configured
            .map(_._1)
            .find(_.domain == domain)
            .toRight(s"No such domain ${domain} configured")
          newConfig = modifier(cfg)
          _ <-
            if (newConfig.domain == cfg.domain) Right(())
            else Left("We don't support modifying the domain alias of a DomainConnectionConfig.")
          _ <- adminCommand(
            ParticipantAdminCommands.DomainConnectivity.ModifyDomainConnection(modifier(cfg))
          ).toEither
        } yield ()
      }
    }

    @Help.Summary(
      "Get the service agreement of the given domain alias and if it has been accepted already."
    )
    def get_agreement(domainAlias: DomainAlias): Option[(v0.Agreement, Boolean)] =
      consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.DomainConnectivity.GetAgreement(domainAlias))
      }
    @Help.Summary("Accept the service agreement of the given domain alias")
    def accept_agreement(domainAlias: DomainAlias, agreementId: String): Unit =
      consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.DomainConnectivity.AcceptAgreement(domainAlias, agreementId)
        )
      }

  }

  @Help.Summary("Composability related functionality", FeatureFlag.Preview)
  @Help.Group("Transfer")
  object transfer extends Helpful {
    @Help.Summary(
      "Transfer-out a contract from the source domain with destination target domain",
      FeatureFlag.Preview,
    )
    @Help.Description(
      """Transfers the given contract out of the source domain with destination target domain.
       The command returns the ID of the transfer when the transfer-out has completed successfully.
       The contract is in transit until the transfer-in has completed on the target domain.
       The submitting party must be a stakeholder of the contract and the participant must have submission rights
       for the submitting party on the source domain. It must also be connected to the target domain.
       An application-id can be specified to uniquely identify the application that have issued the transfer,
       otherwise the default value will be used. An optional submission id can be set by the committer to the value
       of their choice that allows an application to correlate completions to its submissions."""
    )
    def out(
        submittingParty: PartyId,
        contractId: LfContractId,
        sourceDomain: DomainAlias,
        targetDomain: DomainAlias,
        applicationId: LedgerApplicationId = LedgerApplicationId.assertFromString("AdminConsole"),
        submissionId: String = "",
        workflowId: String = "",
        commandId: String = "",
    ): TransferId =
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.Transfer
            .TransferOut(
              submittingParty,
              contractId,
              sourceDomain,
              targetDomain,
              applicationId = applicationId,
              submissionId = submissionId,
              workflowId = workflowId,
              commandId = if (commandId.isEmpty) UUID.randomUUID().toString else commandId,
            )
        )
      })

    @Help.Summary("Transfer-in a contract in transit to the target domain", FeatureFlag.Preview)
    @Help.Description("""Manually transfers a contract in transit into the target domain.
      The command returns when the transfer-in has completed successfully.
      If the transferExclusivityTimeout in the target domain's parameters is set to a positive value,
      all participants of all stakeholders connected to both origin and target domain will attempt to transfer-in
      the contract automatically after the exclusivity timeout has elapsed.
      An application-id can be specified to uniquely identifies the application that have issued the transfer,
      otherwise the default value will be used. An optional submission id can be set by the committer to the value
      of their choice that allows an application to correlate completions to its submissions.""")
    def in(
        submittingParty: PartyId,
        transferId: TransferId,
        targetDomain: DomainAlias,
        applicationId: LedgerApplicationId = LedgerApplicationId.assertFromString("AdminConsole"),
        submissionId: String = "",
        workflowId: String = "",
        commandId: String = "",
    ): Unit =
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.Transfer
            .TransferIn(
              submittingParty,
              transferId.toAdminProto,
              targetDomain,
              applicationId = applicationId,
              submissionId = submissionId,
              workflowId = workflowId,
              commandId = if (commandId.isEmpty) UUID.randomUUID().toString else commandId,
            )
        )
      })

    @Help.Summary("Search the currently in-flight transfers", FeatureFlag.Preview)
    @Help.Description(
      "Returns all in-flight transfers with the given target domain that match the filters, but no more than the limit specifies."
    )
    def search(
        targetDomain: DomainAlias,
        filterSourceDomain: Option[DomainAlias],
        filterTimestamp: Option[Instant],
        filterSubmittingParty: Option[PartyId],
        limit: PositiveInt = defaultLimit,
    ): Seq[TransferSearchResult] =
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.Transfer
            .TransferSearch(
              targetDomain,
              filterSourceDomain,
              filterTimestamp,
              filterSubmittingParty,
              limit.value,
            )
        )
      })

    @Help.Summary(
      "Transfer the contract from the origin domain to the target domain",
      FeatureFlag.Preview,
    )
    @Help.Description(
      "Macro that first calls transfer_out and then transfer_in. No error handling is done."
    )
    def execute(
        submittingParty: PartyId,
        contractId: LfContractId,
        sourceDomain: DomainAlias,
        targetDomain: DomainAlias,
    ): Unit = {
      val transferId = out(submittingParty, contractId, sourceDomain, targetDomain)
      in(submittingParty, transferId, targetDomain)
    }

    @Help.Summary("Lookup the active domain for the provided contracts", FeatureFlag.Preview)
    def lookup_contract_domain(contractIds: LfContractId*): Map[LfContractId, String] =
      check(FeatureFlag.Preview) {
        consoleEnvironment.run {
          adminCommand(ParticipantAdminCommands.Inspection.LookupContractDomain(contractIds.toSet))
        }
      }
  }

  @Help.Summary("Functionality for managing resources")
  @Help.Group("Resource Management")
  object resources extends Helpful {

    @Help.Summary("Set resource limits for the participant.")
    @Help.Description(
      """While a resource limit is attained or exceeded, the participant will reject any additional submission with GRPC status ABORTED.
        |Most importantly, a submission will be rejected **before** it consumes a significant amount of resources.
        |
        |There are three kinds of limits: `maxDirtyRequests`,  `maxRate` and `maxBurstFactor`.
        |The number of dirty requests of a participant P covers (1) requests initiated by P as well as
        |(2) requests initiated by participants other than P that need to be validated by P.
        |Compared to the maximum rate, the maximum number of dirty requests reflects the load on the participant more accurately.
        |However, the maximum number of dirty requests alone does not protect the system from "bursts":
        |If an application submits a huge number of commands at once, the maximum number of dirty requests will likely
        |be exceeded, as the system is registering dirty requests only during validation and not already during
        |submission.
        |
        |The maximum rate is a hard limit on the rate of commands submitted to this participant through the ledger API.
        |As the rate of commands is checked and updated immediately after receiving a new command submission,
        |an application cannot exceed the maximum rate.
        |
        |The `maxBurstFactor` parameter (positive, default 0.5) allows to configure how permissive the rate limitation should be
        |with respect to bursts. The rate limiting will be enforced strictly after having observed `max_burst` * `max_rate` commands.
        |
        |For the sake of illustration, let's assume the configured rate limit is ``100 commands/s`` with a burst ratio of 0.5.
        |If an application submits 100 commands within a single second, waiting exactly 10 milliseconds between consecutive commands,
        |then the participant will accept all commands.
        |With a `maxBurstFactor` of 0.5, the participant will accept the first 50 commands and reject the remaining 50.
        |If the application then waits another 500 ms, it may submit another burst of 50 commands. If it waits 250 ms,
        |it may submit only a burst of 25 commands.
        |
        |Resource limits can only be changed, if the server runs Canton enterprise.
        |In the community edition, the server uses fixed limits that cannot be changed."""
    )
    def set_resource_limits(limits: ResourceLimits): Unit =
      consoleEnvironment.run { adminCommand(SetResourceLimits(limits)) }

    @Help.Summary("Get the resource limits of the participant.")
    def resource_limits(): ResourceLimits = consoleEnvironment.run {
      adminCommand(GetResourceLimits())
    }

  }
}

trait ParticipantHealthAdministrationCommon extends FeatureFlagFilter {
  this: HealthAdministrationCommon[ParticipantStatus] =>

  protected def runner: AdminCommandRunner

  // Single internal implementation so that `maybe_ping`
  // can be hidden behind the `testing` feature flag
  private def ping_internal(
      participantId: ParticipantId,
      timeout: NonNegativeDuration,
      workflowId: String,
      id: String,
  ): Option[Duration] =
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.Ping
          .Ping(
            Set[String](participantId.adminParty.toLf),
            Set(),
            timeout.asFiniteApproximation.toMillis,
            0,
            0,
            workflowId,
            id,
          )
      )
    }

  @Help.Summary(
    "Sends a ping to the target participant over the ledger. " +
      "Yields the duration in case of success and throws a RuntimeException in case of failure."
  )
  def ping(
      participantId: ParticipantId,
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ping,
      workflowId: String = "",
      id: String = "",
  ): Duration = {
    val adminApiRes: Option[Duration] = ping_internal(participantId, timeout, workflowId, id)
    consoleEnvironment.runE(
      adminApiRes.toRight(
        s"Unable to ping $participantId within ${LoggerUtil.roundDurationForHumans(timeout.duration)}"
      )
    )

  }

  @Help.Summary(
    "Sends a ping to the target participant over the ledger. Yields Some(duration) in case of success and None in case of failure.",
    FeatureFlag.Testing,
  )
  def maybe_ping(
      participantId: ParticipantId,
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ping,
      workflowId: String = "",
      id: String = "",
  ): Option[Duration] = check(FeatureFlag.Testing) {
    ping_internal(participantId, timeout, workflowId, id)
  }
}

class ParticipantHealthAdministration(
    val runner: AdminCommandRunner,
    val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends HealthAdministration(
      runner,
      consoleEnvironment,
      ParticipantStatus.fromProtoV0,
    )
    with FeatureFlagFilter
    with ParticipantHealthAdministrationCommon

class ParticipantHealthAdministrationX(
    val runner: AdminCommandRunner,
    val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends HealthAdministrationX(
      runner,
      consoleEnvironment,
      ParticipantStatus.fromProtoV0,
    )
    with FeatureFlagFilter
    with ParticipantHealthAdministrationCommon
