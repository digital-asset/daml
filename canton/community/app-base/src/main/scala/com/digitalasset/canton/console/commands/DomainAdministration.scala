// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.admin.api.client.commands.DomainAdminCommands.GetDomainParameters
import com.digitalasset.canton.admin.api.client.commands.{
  DomainAdminCommands,
  TopologyAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.{
  DynamicDomainParameters,
  DynamicDomainParametersV0,
  DynamicDomainParametersV1,
  ListParticipantDomainStateResult,
  StaticDomainParameters,
  StaticDomainParametersV0,
  StaticDomainParametersV1,
  StaticDomainParametersV2,
}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{
  ConsoleCommandTimeout,
  NonNegativeDuration,
  NonNegativeFiniteDuration,
  PositiveDurationSeconds,
}
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlagFilter,
  Help,
  Helpful,
}
import com.digitalasset.canton.domain.service.ServiceAgreementAcceptance
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.topology.TopologyManagerError.IncreaseOfLedgerTimeRecordTimeTolerance
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.BaseQuery
import com.digitalasset.canton.topology.store.{TimeQuery, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.chaining.scalaUtilChainingOps

trait DomainAdministration {
  this: AdminCommandRunner with FeatureFlagFilter with NamedLogging =>
  protected val consoleEnvironment: ConsoleEnvironment

  def id: DomainId
  def topology: TopologyAdministrationGroup
  protected def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  type Status <: NodeStatus.Status
  def health: HealthAdministration[Status]

  // The DomainTopologyTransactionMessage is about 2500 bytes and each recipient about 100 bytes.
  // with this minimum we can have up to 275 recipients for a domain transaction change.
  private val minimumMaxRequestSizeBytes = NonNegativeInt.tryCreate(30000)

  @Help.Summary("Manage participant permissions")
  @Help.Group("Participants")
  object participants extends Helpful {

    @Help.Summary("List participant states")
    @Help.Description(
      """This command will list the currently valid state as stored in the authorized store.
        | For a deep inspection of the identity management history, use the `topology.participant_domain_states.list` command."""
    )
    def list(): Seq[ListParticipantDomainStateResult] = {
      consoleEnvironment
        .run {
          adminCommand(
            TopologyAdminCommands.Read.ListParticipantDomainState(
              BaseQuery(
                filterStore = TopologyStoreId.AuthorizedStore.filterName,
                useStateStore = false,
                ops = None,
                timeQuery = TimeQuery.HeadState,
                filterSigningKey = "",
                protocolVersion = None,
              ),
              filterDomain = "",
              filterParticipant = "",
            )
          )
        }
        .filter(_.item.side != RequestSide.To)
    }

    @Help.Summary("Change state and trust level of participant")
    @Help.Description("""Set the state of the participant within the domain.
    Valid permissions are 'Submission', 'Confirmation', 'Observation' and 'Disabled'.
    Valid trust levels are 'Vip' and 'Ordinary'.
    Synchronize timeout can be used to ensure that the state has been propagated into the node
    """)
    def set_state(
        participant: ParticipantId,
        permission: ParticipantPermission,
        trustLevel: TrustLevel = TrustLevel.Ordinary,
        synchronize: Option[NonNegativeDuration] = Some(timeouts.bounded),
    ): Unit = {
      val _ = consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write.AuthorizeParticipantDomainState(
            TopologyChangeOp.Add,
            None,
            RequestSide.From,
            id,
            participant,
            permission,
            trustLevel,
            replaceExisting = true,
          )
        )
      }
      synchronize.foreach(topology.synchronisation.await_idle)
    }

    @Help.Summary("Test whether a participant is permissioned on this domain")
    def active(participantId: ParticipantId): Boolean =
      topology.participant_domain_states.active(id, participantId)
  }

  @Help.Summary("Domain service commands")
  @Help.Group("Service")
  object service extends Helpful {

    @Help.Summary("List the accepted service agreements")
    def list_accepted_agreements(): Seq[ServiceAgreementAcceptance] =
      consoleEnvironment.run(adminCommand(DomainAdminCommands.ListAcceptedServiceAgreements))

    @Help.Summary("Get the Static Domain Parameters configured for the domain")
    def get_static_domain_parameters: StaticDomainParameters =
      consoleEnvironment.run(
        adminCommand(GetDomainParameters()(consoleEnvironment.environment.executionContext))
      )

    @Help.Summary("Get the Dynamic Domain Parameters configured for the domain")
    def get_dynamic_domain_parameters: DynamicDomainParameters = topology.domain_parameters_changes
      .list("Authorized")
      .sortBy(_.context.validFrom)(implicitly[Ordering[java.time.Instant]].reverse)
      .headOption
      .map(_.item)
      .getOrElse(
        throw new IllegalStateException("No dynamic domain parameters found in the domain")
      )

    private def get_dynamic_domain_parameters_v1(
        operation: String
    ): DynamicDomainParametersV1 = {
      get_dynamic_domain_parameters match {
        case _: DynamicDomainParametersV0 =>
          val protocolVersion = get_static_domain_parameters.protocolVersion
          throw new UnsupportedOperationException(
            s"Unable to $operation for domain running protocol version $protocolVersion."
          )
        case p: DynamicDomainParametersV1 => p
      }
    }

    /*
      Get a parameter that was static in V0 and dynamic from V1
     */
    private def getParameterStaticV0DynamicV1[P](
        operation: String,
        fromStaticV0: StaticDomainParametersV0 => P,
        fromDynamicV1: DynamicDomainParametersV1 => P,
    ): P = get_dynamic_domain_parameters match {
      case _: DynamicDomainParametersV0 =>
        get_static_domain_parameters match {
          case staticV0: StaticDomainParametersV0 =>
            fromStaticV0(staticV0)

          case _: StaticDomainParametersV1 =>
            throw new IllegalStateException(
              s"Error when trying to get $operation: versions of static and dynamic domains parameters should be consistent but got 1 and 0 respectively"
            )

          case _: StaticDomainParametersV2 =>
            throw new IllegalStateException(
              s"Error when trying to get $operation: versions of static and dynamic domains parameters should be consistent but got 2 and 0 respectively"
            )
        }

      case dynamicV1: DynamicDomainParametersV1 => fromDynamicV1(dynamicV1)
    }

    @Help.Summary("Get the reconciliation interval configured for the domain")
    @Help.Description("""Depending on the protocol version used on the domain, the value will be
        read either from the static domain parameters or the dynamic ones.""")
    def get_reconciliation_interval: PositiveDurationSeconds =
      getParameterStaticV0DynamicV1(
        "reconciliation interval",
        _.reconciliationInterval,
        _.reconciliationInterval,
      )

    @Help.Summary("Get the max rate per participant")
    @Help.Description("""Depending on the protocol version used on the domain, the value will be
        read either from the static domain parameters or the dynamic ones.""")
    def get_max_rate_per_participant: NonNegativeInt =
      getParameterStaticV0DynamicV1(
        "max rate per participant",
        _.maxRatePerParticipant,
        _.maxRatePerParticipant,
      )

    @Help.Summary("Get the max request size")
    @Help.Description("""Depending on the protocol version used on the domain, the value will be
        read either from the static domain parameters or the dynamic ones.
        This value is not necessarily the one used by the sequencer node because it requires a restart
        of the server to be taken into account.""")
    def get_max_request_size: NonNegativeInt =
      TraceContext.withNewTraceContext { implicit tc =>
        getParameterStaticV0DynamicV1(
          "max request size",
          _.maxInboundMessageSize,
          _.maxRequestSize,
        ).tap { res =>
          logger.info(
            s"This value ($res) is not necessarily the one used by the sequencer node because it requires a restart of the server to be taken into account"
          )
        }
      }

    @Help.Summary("Get the mediator deduplication timeout")
    @Help.Description(
      "The method will fail, if the domain does not support the mediatorDeduplicationTimeout."
    )
    @Help.AvailableFrom(ProtocolVersion.v4)
    def get_mediator_deduplication_timeout: NonNegativeFiniteDuration =
      get_dynamic_domain_parameters_v1(
        "get mediatorDeduplicationTimeout"
      ).mediatorDeduplicationTimeout

    @Help.Summary("Update the mediator deduplication timeout")
    @Help.Description(
      """The method will fail:
        |
        |- if the domain does not support the ``mediatorDeduplicationTimeout`` parameter,
        |- if the new value of ``mediatorDeduplicationTimeout`` is less than twice the value of ``ledgerTimeRecordTimeTolerance.``"""
    )
    @Help.AvailableFrom(ProtocolVersion.v4)
    def set_mediator_deduplication_timeout(
        newMediatorDeduplicationTimeout: NonNegativeFiniteDuration
    ): Unit =
      update_dynamic_domain_parameters_v1(
        _.copy(mediatorDeduplicationTimeout = newMediatorDeduplicationTimeout),
        "set mediatorDeduplicationTimeout",
      )

    @Help.Summary("Set the Dynamic Domain Parameters configured for the domain")
    @Help.Description(
      """force: Enable potentially dangerous changes. Required to increase ``ledgerTimeRecordTimeTolerance``.
        |Use ``set_ledger_time_record_time_tolerance`` to securely increase ``ledgerTimeRecordTimeTolerance``."""
    )
    def set_dynamic_domain_parameters(
        dynamicDomainParameters: DynamicDomainParameters,
        force: Boolean = false,
    ): Unit = {
      val protocolVersion = get_static_domain_parameters.protocolVersion
      topology.domain_parameters_changes
        .authorize(id, dynamicDomainParameters, protocolVersion, force = force)
        .discard[ByteString]
    }

    @Help.Summary("Update the Dynamic Domain Parameters for the domain")
    @Help.Description(
      """force: Enable potentially dangerous changes. Required to increase ``ledgerTimeRecordTimeTolerance``.
        |Use ``set_ledger_time_record_time_tolerance_securely`` to securely increase ``ledgerTimeRecordTimeTolerance``."""
    )
    def update_dynamic_domain_parameters(
        modifier: DynamicDomainParameters => DynamicDomainParameters,
        force: Boolean = false,
    ): Unit = {
      val currentDomainParameters = get_dynamic_domain_parameters
      val protocolVersion = get_static_domain_parameters.protocolVersion

      val newDomainParameters = modifier(currentDomainParameters)
      topology.domain_parameters_changes
        .authorize(id, newDomainParameters, protocolVersion, force = force)
        .discard[ByteString]
    }

    @Help.Summary("Update the Dynamic Domain Parameters for the domain")
    @Help.Description(
      """force: Enable potentially dangerous changes. Required to increase ``ledgerTimeRecordTimeTolerance``.
        |Use ``set_ledger_time_record_time_tolerance_securely`` to securely increase ``ledgerTimeRecordTimeTolerance``."""
    )
    @deprecated("Use set_max_request_size instead", "2.5.0")
    def update_dynamic_parameters(
        modifier: DynamicDomainParameters => DynamicDomainParameters,
        force: Boolean = false,
    ): Unit = update_dynamic_domain_parameters(modifier, force)

    @Help.Summary("Try to update the reconciliation interval for the domain")
    @Help.Description("""If the reconciliation interval is dynamic, update the value.
        If the reconciliation interval is not dynamic (i.e., if the domain is running
        on protocol version lower than `4`), then it will throw an error.
        """)
    @Help.AvailableFrom(ProtocolVersion.v4)
    def set_reconciliation_interval(
        newReconciliationInterval: PositiveDurationSeconds
    ): Unit =
      update_dynamic_domain_parameters_v1(
        _.copy(reconciliationInterval = newReconciliationInterval),
        "update reconciliation interval",
      )

    @Help.Summary("Try to update the max rate per participant for the domain")
    @Help.Description("""If the max rate per participant is dynamic, update the value.
        If the max rate per participant is not dynamic (i.e., if the domain is running
        on protocol version lower than `4`),  then it will throw an error.
        """)
    @Help.AvailableFrom(ProtocolVersion.v4)
    def set_max_rate_per_participant(
        maxRatePerParticipant: NonNegativeInt
    ): Unit =
      update_dynamic_domain_parameters_v1(
        _.copy(maxRatePerParticipant = maxRatePerParticipant),
        "update max rate per participant",
      )

    @Help.Summary("Try to update the max rate per participant for the domain")
    @Help.Description("""If the max request size is dynamic, update the value.
                         The update won't have any effect unless the sequencer server is restarted.
    If the max request size is not dynamic (i.e., if the domain is running
    on protocol version lower than `4`), then it will throw an error.
    """)
    @Help.AvailableFrom(ProtocolVersion.v4)
    @deprecated("Use set_max_request_size instead", "2.5.0")
    def set_max_inbound_message_size(
        maxRequestSize: NonNegativeInt,
        force: Boolean = false,
    ): Unit = set_max_request_size(maxRequestSize, force)

    @Help.Summary("Try to update the max rate per participant for the domain")
    @Help.Description("""If the max request size is dynamic, update the value.
                         The update won't have any effect unless the sequencer server is restarted.
    If the max request size is not dynamic (i.e., if the domain is running
    on protocol version lower than `4`), then it will throw an error.
    """)
    @Help.AvailableFrom(ProtocolVersion.v4)
    def set_max_request_size(
        maxRequestSize: NonNegativeInt,
        force: Boolean = false,
    ): Unit =
      TraceContext.withNewTraceContext { implicit tc =>
        if (maxRequestSize < minimumMaxRequestSizeBytes && !force)
          logger.warn(
            s"""|The maxRequestSize requested is lower than the minimum advised value ($minimumMaxRequestSizeBytes) which can crash Canton.
                |To set this value anyway, set force to true.""".stripMargin
          )
        else
          update_dynamic_domain_parameters_v1(
            _.copy(maxRequestSize = maxRequestSize),
            "update max request size",
          )
        logger.info(
          "Please restart the sequencer node to take into account the new value for max-request-size."
        )
      }

    private def update_dynamic_domain_parameters_v1(
        modifier: DynamicDomainParametersV1 => DynamicDomainParametersV1,
        operation: String,
        force: Boolean = false,
    ): Unit = {
      val currentDomainParameters = get_dynamic_domain_parameters_v1(operation)
      val protocolVersion = get_static_domain_parameters.protocolVersion

      val newDomainParameters = modifier(currentDomainParameters)
      topology.domain_parameters_changes
        .authorize(id, newDomainParameters, protocolVersion, force = force)
        .discard[ByteString]
    }

    @Help.Summary(
      "Update the `ledgerTimeRecordTimeTolerance` in the dynamic domain parameters."
    )
    @Help.Description(
      """If it would be insecure to perform the change immediately,
        |the command will block and wait until it is secure to perform the change.
        |The command will block for at most twice of ``newLedgerTimeRecordTimeTolerance``.
        |
        |If the domain does not support ``mediatorDeduplicationTimeout``,
        |the method will update ``ledgerTimeRecordTimeTolerance`` immediately without blocking.
        |
        |The method will fail if ``mediatorDeduplicationTimeout`` is less than twice of ``newLedgerTimeRecordTimeTolerance``.
        |
        |Do not modify domain parameters concurrently while running this command,
        |because the command may override concurrent changes.
        |
        |force: update ``ledgerTimeRecordTimeTolerance`` immediately without blocking.
        |This is safe to do during domain bootstrapping and in test environments, but should not be done in operational production systems.."""
    )
    def set_ledger_time_record_time_tolerance(
        newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
        force: Boolean = false,
    ): Unit = {
      TraceContext.withNewTraceContext { implicit tc =>
        get_dynamic_domain_parameters match {
          case oldDomainParameters: DynamicDomainParametersV1 if !force =>
            securely_set_ledger_time_record_time_tolerance(
              oldDomainParameters,
              newLedgerTimeRecordTimeTolerance,
            )

          case _: DynamicDomainParameters =>
            logger.info(
              s"Immediately updating ledgerTimeRecordTimeTolerance to $newLedgerTimeRecordTimeTolerance..."
            )
            update_dynamic_domain_parameters(
              _.update(ledgerTimeRecordTimeTolerance = newLedgerTimeRecordTimeTolerance),
              force = true,
            )
        }
      }
    }

    private def securely_set_ledger_time_record_time_tolerance(
        oldDomainParameters: DynamicDomainParametersV1,
        newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
    )(implicit traceContext: TraceContext): Unit = {
      implicit val ec: ExecutionContext = consoleEnvironment.environment.executionContext

      // See i9028 for a detailed design.
      // https://docs.google.com/document/d/1tpPbzv2s6bjbekVGBn6X5VZuw0oOTHek5c30CBo4UkI/edit#bookmark=id.1dzc6dxxlpca
      // We wait until the antecedent of Lemma 2 Item 2 is falsified for all changes that violate the conclusion.

      // Compute new parameters
      val oldLedgerTimeRecordTimeTolerance = oldDomainParameters.ledgerTimeRecordTimeTolerance

      val minMediatorDeduplicationTimeout = newLedgerTimeRecordTimeTolerance * 2

      if (oldDomainParameters.mediatorDeduplicationTimeout < minMediatorDeduplicationTimeout) {
        val err = IncreaseOfLedgerTimeRecordTimeTolerance
          .PermanentlyInsecure(
            newLedgerTimeRecordTimeTolerance.toInternal,
            oldDomainParameters.mediatorDeduplicationTimeout.toInternal,
          )
        val msg = CantonError.stringFromContext(err)
        consoleEnvironment.run(GenericCommandError(msg))
      }

      logger.info(
        s"Securely updating ledgerTimeRecordTimeTolerance to $newLedgerTimeRecordTimeTolerance..."
      )

      // Poll until it is safe to increase ledgerTimeRecordTimeTolerance
      def checkPreconditions(): Future[Unit] = {
        val startTs = consoleEnvironment.environment.clock.now

        // Update mediatorDeduplicationTimeout for several reasons:
        // 1. Make sure it is big enough.
        // 2. The resulting topology transaction gives us a meaningful lower bound on the sequencer clock.
        logger.info(
          s"Do a no-op update of ledgerTimeRecordTimeTolerance to $oldLedgerTimeRecordTimeTolerance..."
        )
        update_dynamic_domain_parameters_v1(
          _.copy(ledgerTimeRecordTimeTolerance = oldLedgerTimeRecordTimeTolerance),
          "update ledgerTimeRecordTimeTolerance",
        )

        logger.debug(s"Check for incompatible past domain parameters...")

        val allTransactions = topology.domain_parameters_changes.list(
          id.filterString,
          useStateStore = false,
          // We can't specify a lower bound in range because that would be compared against validFrom.
          // (But we need to compare to validUntil).
          TimeQuery.Range(None, None),
        )

        // This serves as a lower bound of validFrom for the next topology transaction.
        val lastSequencerTs =
          allTransactions
            .map(_.context.validFrom)
            .maxOption
            .getOrElse(throw new NoSuchElementException("Missing domain parameters!"))

        logger.debug(s"Last sequencer timestamp is $lastSequencerTs.")

        // Determine how long we need to wait until all incompatible domainParameters have become
        // invalid for at least minMediatorDeduplicationTimeout.
        val waitDuration = allTransactions
          .filterNot(
            _.item.compatibleWithNewLedgerTimeRecordTimeTolerance(newLedgerTimeRecordTimeTolerance)
          )
          .map { tx =>
            val elapsedForAtLeast = tx.context.validUntil match {
              case Some(validUntil) => Duration.between(validUntil, lastSequencerTs)
              case None => Duration.ZERO
            }
            minMediatorDeduplicationTimeout.asJava minus elapsedForAtLeast
          }
          .maxOption
          .getOrElse(Duration.ZERO)

        if (waitDuration > Duration.ZERO) {
          logger.info(
            show"Found incompatible past domain parameters. Waiting for $waitDuration..."
          )

          // Use the clock instead of Threading.sleep to support sim clock based tests.
          val delayF = consoleEnvironment.environment.clock
            .scheduleAt(
              _ => (),
              startTs.plus(waitDuration),
            ) // avoid scheduleAfter, because that causes a race condition in integration tests
            .onShutdown(
              throw new IllegalStateException(
                "Update of ledgerTimeRecordTimeTolerance interrupted due to shutdown."
              )
            )
          // Do not submit checkPreconditions() to the clock because it is blocking and would therefore block the clock.
          delayF.flatMap(_ => checkPreconditions())
        } else {
          Future.unit
        }
      }

      timeouts.unbounded.await("Wait until ledgerTimeRecordTimeTolerance can be increased.")(
        checkPreconditions()
      )

      // Now that past values of mediatorDeduplicationTimeout have been large enough,
      // we can change ledgerTimeRecordTimeTolerance.
      logger.info(
        s"Now changing ledgerTimeRecordTimeTolerance to $newLedgerTimeRecordTimeTolerance..."
      )
      update_dynamic_domain_parameters_v1(
        _.copy(ledgerTimeRecordTimeTolerance = newLedgerTimeRecordTimeTolerance),
        "update ledgerTimeRecordTimeTolerance",
        force = true,
      )
    }
  }
}
