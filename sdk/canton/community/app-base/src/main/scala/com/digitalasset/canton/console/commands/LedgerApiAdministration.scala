// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.jwt.{AuthServiceJWTCodec, JwksUrl, Jwt, JwtDecoder, StandardJWTPayload}
import com.daml.ledger.api.v2.admin.command_inspection_service.CommandState
import com.daml.ledger.api.v2.admin.package_management_service.PackageDetails
import com.daml.ledger.api.v2.admin.party_management_service.PartyDetails as ProtoPartyDetails
import com.daml.ledger.api.v2.commands.{Command, DisclosedContract, PrefetchContractKey}
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitResponse as ExecuteAndWaitResponseProto,
  ExecuteSubmissionResponse as ExecuteResponseProto,
  GetPreferredPackagesResponse,
  HashingSchemeVersion,
  PackagePreference,
  PrepareSubmissionResponse as PrepareResponseProto,
  PreparedTransaction,
}
import com.daml.ledger.api.v2.reassignment.Reassignment as ReassignmentProto
import com.daml.ledger.api.v2.state_service.{
  ActiveContract,
  GetActiveContractsResponse,
  GetConnectedSynchronizersResponse,
}
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction as TopoplogyTransactionProto
import com.daml.ledger.api.v2.transaction.Transaction as ApiTransaction
import com.daml.ledger.api.v2.transaction_filter
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  ParticipantAuthorizationTopologyFormat,
  TemplateFilter,
  TopologyFormat,
  TransactionFormat as TransactionFormatProto,
  TransactionShape,
  UpdateFormat,
  WildcardFilter,
}
import com.daml.ledger.javaapi as javab
import com.daml.ledger.javaapi.data.{
  GetUpdatesResponse,
  Reassignment,
  TopologyTransaction,
  Transaction,
  TransactionFormat,
}
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.*
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.{
  WrappedContractEntry,
  WrappedIncompleteAssigned,
  WrappedIncompleteUnassigned,
}
import com.digitalasset.canton.admin.api.client.data.*
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  ConsoleMacros,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  LedgerApiCommandRunner,
  LocalParticipantReference,
  ParticipantReference,
  RemoteParticipantReference,
}
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.ledger.api.{IdentityProviderConfig, IdentityProviderId}
import com.digitalasset.canton.ledger.client.services.admin.IdentityProviderConfigClient
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.networking.grpc.{GrpcError, RecordingStreamObserver}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.platform.apiserver.execution.CommandStatus
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.{
  ExternalParty,
  ParticipantId,
  Party,
  PartyId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPartyId, config}
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps

trait BaseLedgerApiAdministration extends NoTracing with StreamingCommandHelper {
  thisAdministration: LedgerApiCommandRunner & NamedLogging & FeatureFlagFilter =>

  implicit protected[canton] lazy val executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext

  implicit protected val consoleEnvironment: ConsoleEnvironment

  protected val name: String

  private[canton] lazy val userId: String = token
    .flatMap(encodedToken => JwtDecoder.decode(Jwt(encodedToken)).toOption)
    .flatMap(decodedToken => AuthServiceJWTCodec.readFromString(decodedToken.payload).toOption)
    .map { case s: StandardJWTPayload => s.userId }
    .getOrElse(LedgerApiCommands.defaultUserId)

  private def eventFormatAllParties(includeCreatedEventBlob: Boolean = false): Option[EventFormat] =
    Some(
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(
          Filters(
            Seq(
              CumulativeFilter(
                IdentifierFilter.WildcardFilter(
                  WildcardFilter(
                    includeCreatedEventBlob = includeCreatedEventBlob
                  )
                )
              )
            )
          )
        ),
        verbose = true,
      )
    )

  private[canton] def optionallyAwait[Tx](
      tx: Tx,
      txId: String,
      txSynchronizerId: String,
      optTimeout: Option[config.NonNegativeDuration],
  ): Tx
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts
  protected def defaultLimit: PositiveInt =
    consoleEnvironment.environment.config.parameters.console.defaultLimit

  @Help.Summary("Group of commands that access the ledger-api")
  @Help.Group("Ledger Api")
  object ledger_api extends Helpful {

    @Help.Summary("Read from update stream")
    @Help.Group("Updates")
    object updates extends Helpful {

      @Help.Summary("Get updates")
      @Help.Description(
        """This function connects to the update stream for the given parties and collects updates
          |until either `completeAfter` updates have been received or `timeout` has elapsed.
          |The returned updates can be filtered to be between the given offsets (default: no filtering).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error.
          |If the beginOffset is zero then the participant begin is taken as beginning offset.
          |If the endOffset is None then a continuous stream is returned."""
      )
      def updates(
          updateFormat: UpdateFormat,
          completeAfter: PositiveInt,
          beginOffsetExclusive: Long = 0L,
          endOffsetInclusive: Option[Long] = None,
          timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
          resultFilter: UpdateWrapper => Boolean = _ => true,
          synchronizerFilter: Option[SynchronizerId] = None,
      ): Seq[UpdateWrapper] = {

        val resultFilterWithSynchronizer = synchronizerFilter match {
          case Some(synchronizerId) =>
            (update: UpdateWrapper) =>
              resultFilter(update) && update.synchronizerId == synchronizerId.toProtoPrimitive
          case None => resultFilter
        }

        val observer =
          new RecordingStreamObserver[UpdateWrapper](completeAfter, resultFilterWithSynchronizer)

        mkResult(
          subscribe_updates(
            observer,
            updateFormat,
            beginOffsetExclusive,
            endOffsetInclusive,
          ),
          "getUpdates",
          observer,
          timeout,
        )
      }

      @Help.Summary("Get transactions")
      @Help.Description(
        """This function connects to the update stream for the given parties and collects updates
          |until either `completeAfter` transactions have been received or `timeout` has elapsed.
          |The returned updates can be filtered to be between the given offsets (default: no filtering).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error. If you need to specify filtering conditions for template IDs and
          |including create event blobs for explicit disclosure, consider using `tx_with_tx_format`.
          |If the beginOffset is zero then the participant begin is taken as beginning offset.
          |If the endOffset is None then a continuous stream is returned."""
      )
      def transactions(
          partyIds: Set[Party],
          completeAfter: PositiveInt,
          beginOffsetExclusive: Long = 0L,
          endOffsetInclusive: Option[Long] = None,
          verbose: Boolean = true,
          timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
          resultFilter: UpdateWrapper => Boolean = _ => true,
          synchronizerFilter: Option[SynchronizerId] = None,
          transactionShape: TransactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          includeCreatedEventBlob: Boolean = false,
      ): Seq[TransactionWrapper] = {

        val resultFilterWithSynchronizer = synchronizerFilter match {
          case Some(synchronizerId) =>
            (update: UpdateWrapper) =>
              resultFilter(update) && update.synchronizerId == synchronizerId.toProtoPrimitive
          case None => resultFilter
        }

        val observer =
          new RecordingStreamObserver[UpdateWrapper](completeAfter, resultFilterWithSynchronizer)

        val transactionFormat = TransactionFormatProto(
          eventFormat = Some(
            EventFormat(
              filtersByParty = partyIds
                .map(
                  _.toLf -> Filters(
                    Seq(
                      CumulativeFilter.of(
                        IdentifierFilter.WildcardFilter(
                          WildcardFilter(includeCreatedEventBlob = includeCreatedEventBlob)
                        )
                      )
                    )
                  )
                )
                .toMap,
              filtersForAnyParty = None,
              verbose = verbose,
            )
          ),
          transactionShape = transactionShape,
        )

        mkResult(
          subscribe_updates(
            observer,
            UpdateFormat(
              includeTransactions = Some(transactionFormat),
              includeReassignments = None,
              includeTopologyEvents = None,
            ),
            beginOffsetExclusive,
            endOffsetInclusive,
          ),
          "getUpdates",
          observer,
          timeout,
        ).collect { case tx: TransactionWrapper => tx }
      }

      @Help.Summary("Get reassignments")
      @Help.Description(
        """This function connects to the update stream for the given parties and template ids and collects reassignment
          |events (assigned and unassigned) until either `completeAfter` updates have been received or `timeout` has
          |elapsed.
          |If the party ids set is empty then the reassignments for all the parties will be fetched.
          |If the template ids collection is empty then the reassignments for all the template ids will be fetched.
          |The returned updates can be filtered to be between the given offsets (default: no filtering).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error.
          |If the beginOffset is zero then the participant begin is taken as beginning offset.
          |If the endOffset is None then a continuous stream is returned."""
      )
      def reassignments(
          partyIds: Set[PartyId],
          filterTemplates: Seq[TemplateId],
          completeAfter: PositiveInt,
          beginOffsetExclusive: Long = 0L,
          endOffsetInclusive: Option[Long] = None,
          verbose: Boolean = false,
          timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
          resultFilter: UpdateWrapper => Boolean = _ => true,
          synchronizerFilter: Option[SynchronizerId] = None,
          includeCreatedEventBlob: Boolean = false,
      ): Seq[ReassignmentWrapper] = {

        val resultFilterWithSynchronizer = synchronizerFilter match {
          case Some(synchronizerId) =>
            (update: UpdateWrapper) =>
              update match {
                case _: ReassignmentWrapper =>
                  resultFilter(update) && update.synchronizerId == synchronizerId.toProtoPrimitive

                case _ => false
              }
          case None => resultFilter
        }

        val observer =
          new RecordingStreamObserver[UpdateWrapper](completeAfter, resultFilterWithSynchronizer)

        val filters: Filters = Filters(
          if (filterTemplates.isEmpty)
            Seq(
              CumulativeFilter(
                IdentifierFilter.WildcardFilter(
                  WildcardFilter(includeCreatedEventBlob = includeCreatedEventBlob)
                )
              )
            )
          else
            filterTemplates.map(templateId =>
              CumulativeFilter(
                IdentifierFilter.TemplateFilter(
                  TemplateFilter(
                    Some(templateId.toIdentifier),
                    includeCreatedEventBlob = includeCreatedEventBlob,
                  )
                )
              )
            )
        )

        val updateFormat = UpdateFormat(
          includeReassignments =
            if (partyIds.isEmpty)
              Some(
                EventFormat(
                  filtersByParty = Map.empty,
                  filtersForAnyParty = Some(filters),
                  verbose = verbose,
                )
              )
            else
              Some(
                EventFormat(
                  filtersByParty = partyIds.map(_.toLf -> filters).toMap,
                  filtersForAnyParty = None,
                  verbose = verbose,
                )
              ),
          includeTransactions = None,
          includeTopologyEvents = None,
        )

        mkResult(
          subscribe_updates(
            observer = observer,
            updateFormat = updateFormat,
            beginOffsetExclusive = beginOffsetExclusive,
            endOffsetInclusive = endOffsetInclusive,
          ),
          "getUpdates",
          observer,
          timeout,
        ).collect { case reassignment: ReassignmentWrapper => reassignment }
      }

      @Help.Summary("Get topology transactions")
      @Help.Description(
        """This function connects to the update stream for the given parties and collects topology transaction
          |events until either `completeAfter` updates have been received or `timeout` has elapsed.
          |If the party ids seq is empty then the topology transactions for all the parties will be fetched.
          |The returned updates can be filtered to be between the given offsets (default: no filtering).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error.
          |If the beginOffset is zero then the participant begin is taken as beginning offset.
          |If the endOffset is None then a continuous stream is returned."""
      )
      def topology_transactions(
          completeAfter: PositiveInt,
          partyIds: Seq[Party] = Seq.empty,
          beginOffsetExclusive: Long = 0L,
          endOffsetInclusive: Option[Long] = None,
          timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
          resultFilter: UpdateWrapper => Boolean = _ => true,
          synchronizerFilter: Option[SynchronizerId] = None,
      ): Seq[TopologyTransactionWrapper] = {

        val resultFilterWithSynchronizer = synchronizerFilter match {
          case Some(synchronizerId) =>
            (update: UpdateWrapper) =>
              update match {
                case _: TopologyTransactionWrapper =>
                  resultFilter(update) && update.synchronizerId == synchronizerId.toProtoPrimitive

                case _ => false
              }

          case None => resultFilter
        }

        val observer =
          new RecordingStreamObserver[UpdateWrapper](completeAfter, resultFilterWithSynchronizer)
        val updateFormat = UpdateFormat(
          includeTransactions = None,
          includeReassignments = None,
          includeTopologyEvents = Some(
            TopologyFormat(
              includeParticipantAuthorizationEvents = Some(
                ParticipantAuthorizationTopologyFormat(
                  parties = partyIds.map(_.toLf)
                )
              )
            )
          ),
        )

        mkResult(
          subscribe_updates(
            observer = observer,
            updateFormat = updateFormat,
            beginOffsetExclusive = beginOffsetExclusive,
            endOffsetInclusive = endOffsetInclusive,
          ),
          "getUpdates",
          observer,
          timeout,
        ).collect { case wrapper: TopologyTransactionWrapper => wrapper }
      }

      @Help.Summary("Get updates")
      @Help.Description(
        """This function connects to the update stream for the given transaction format and collects updates
          |until either `completeAfter` transactions have been received or `timeout` has elapsed.
          |The returned transactions can be filtered to be between the given offsets (default: no filtering).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error. If you only need to filter by a set of parties, consider using
          |`transactions` instead.
          |If the beginOffset is zero then the participant begin is taken as beginning offset.
          |If the endOffset is None then a continuous stream is returned."""
      )
      def transactions_with_tx_format(
          transactionFormat: TransactionFormatProto,
          completeAfter: PositiveInt,
          beginOffsetExclusive: Long = 0L,
          endOffsetInclusive: Option[Long] = None,
          timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
          resultFilter: UpdateWrapper => Boolean = _ => true,
      ): Seq[UpdateWrapper] = {
        val observer = new RecordingStreamObserver[UpdateWrapper](completeAfter, resultFilter)
        mkResult(
          subscribe_updates(
            observer,
            UpdateFormat(
              includeTransactions = Some(transactionFormat),
              includeReassignments = None,
              includeTopologyEvents = None,
            ),
            beginOffsetExclusive,
            endOffsetInclusive,
          ),
          "getUpdates",
          observer,
          timeout,
        )
      }

      @Help.Summary("Subscribe to the update stream")
      @Help.Description("""This function connects to the update stream and passes updates to `observer` until the stream
          |is completed.
          |The updates as described in the update format will be returned.
          |Use `EventFormat(Map(myParty.toLf -> Filters()))` to return transactions or reassignments for
          |`myParty: PartyId`.
          |The returned updates can be filtered to be between the given offsets (default: no filtering).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error.
          |If the beginOffset is zero then the participant begin is taken as beginning offset.
          |If the endOffset is None then a continuous stream is returned.""")
      def subscribe_updates(
          observer: StreamObserver[UpdateWrapper],
          updateFormat: UpdateFormat,
          beginOffsetExclusive: Long = 0L,
          endOffsetInclusive: Option[Long] = None,
      ): AutoCloseable =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.UpdateService.SubscribeUpdates(
              observer = observer,
              beginExclusive = beginOffsetExclusive,
              endInclusive = endOffsetInclusive,
              updateFormat = updateFormat,
            )
          )
        }

      @Help.Summary("Starts measuring throughput at the update service")
      @Help.Description(
        """This function will subscribe on behalf of `parties` to the update stream and
          |notify various metrics:
          |The metric `<name>.<metricSuffix>` counts the number of update trees emitted.
          |The metric `<name>.<metricSuffix>-tx-node-count` tracks the number of events emitted as part of update.
          |The metric `<name>.<metricSuffix>-tx-size` tracks the number of bytes emitted as part of update trees.
          |
          |To stop measuring, you need to close the returned `AutoCloseable`.
          |Use the `onUpdate` parameter to register a callback that is called on every update tree."""
      )
      def start_measuring(
          parties: Set[Party],
          metricName: String,
          onUpdate: UpdateWrapper => Unit = _ => (),
      )(implicit consoleEnvironment: ConsoleEnvironment): AutoCloseable = {
        val observer: StreamObserver[UpdateWrapper] = new StreamObserver[UpdateWrapper] {

          implicit val metricsContext: MetricsContext =
            MetricsContext("measurement" -> metricName)

          private val consoleMetrics = consoleEnvironment.environment.metricsRegistry
            .forParticipant(name)
            .consoleThroughput

          override def onNext(tx: UpdateWrapper): Unit = {
            val (s, serializedSize) = tx match {
              case TransactionWrapper(transaction) =>
                Transaction
                  .fromProto(ApiTransaction.toJavaProto(transaction))
                  .getRootNodeIds
                  .size
                  .toLong -> transaction.serializedSize
              case reassignmentWrapper: ReassignmentWrapper =>
                1L -> reassignmentWrapper.reassignment.serializedSize
              case topologyTransaction: TopologyTransactionWrapper =>
                throw new RuntimeException(
                  s"Unexpectedly received a topology transaction: $topologyTransaction."
                )
            }
            consoleMetrics.metric.mark(s)
            consoleMetrics.nodeCount.update(s)
            consoleMetrics.transactionSize.update(serializedSize)
            onUpdate(tx)
          }

          override def onError(t: Throwable): Unit = t match {
            case t: StatusRuntimeException =>
              val err = GrpcError("start_measuring", name, t)
              err match {
                case gaveUp: GrpcError.GrpcClientGaveUp if gaveUp.isClientCancellation =>
                  logger.info(s"Client cancelled measuring throughput (metric: $metricName).")
                case _ =>
                  logger.warn(
                    s"An error occurred while measuring throughput (metric: $metricName). Stop measuring. $err"
                  )
              }
            case _: Throwable =>
              logger.warn(
                s"An exception occurred while measuring throughput (metric: $metricName). Stop measuring.",
                t,
              )
          }

          override def onCompleted(): Unit =
            logger.info(s"Stop measuring throughput (metric: $metricName).")
        }

        val eventFormat = EventFormat(
          filtersByParty = parties.map(_.toLf -> Filters(Nil)).toMap,
          filtersForAnyParty = None,
          verbose = false,
        )
        val updateFormat = UpdateFormat(
          includeTransactions = Some(
            TransactionFormatProto(
              eventFormat = Some(eventFormat),
              transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
            )
          ),
          includeReassignments = Some(eventFormat),
          includeTopologyEvents = None,
        )

        logger.info(s"Start measuring throughput (metric: $metricName).")
        subscribe_updates(
          observer = observer,
          updateFormat = updateFormat,
          beginOffsetExclusive = state.end(),
        )
      }

      @Help.Summary("Get an update by its ID")
      @Help.Description(
        """Get an update by its ID. Returns None if the update is not (yet) known at the participant or all the events
          |of the update are filtered due to the update format or if the update has been pruned via `pruning.prune`."""
      )
      def update_by_id(id: String, updateFormat: UpdateFormat): Option[UpdateWrapper] =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.UpdateService.GetUpdateById(id, updateFormat)(
              consoleEnvironment.environment.executionContext
            )
          )
        }

      @Help.Summary("Get an update by its offset")
      @Help.Description(
        """Get an update by its offset. Returns None if the update is not (yet) known at the participant or all the
          |events of the update are filtered due to the update format or if the update has been pruned via
          |`pruning.prune`."""
      )
      def update_by_offset(
          offset: Long,
          updateFormat: UpdateFormat,
      ): Option[UpdateWrapper] =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.UpdateService.GetUpdateByOffset(offset, updateFormat)(
              consoleEnvironment.environment.executionContext
            )
          )
        }
    }

    @Help.Summary("Interactive submission")
    @Help.Group("Interactive Submission")
    object interactive_submission extends Helpful {

      @Help.Summary(
        """Prepare a transaction for interactive submission
           Note that the hash in the response is provided for convenience. Callers should re-compute the hash
           of the transactions (and possibly compare it to the provided one) before signing it.
          """
      )
      @Help.Description(
        """Prepare a transaction for interactive submission.
          |Similar to submit, except instead of submitting the transaction to the network,
          |a serialized version of the transaction will be returned, along with a hash.
          |This allows non-hosted parties to sign the hash with they private key before submitting it via the
          |execute command. If you wish to directly submit a command instead without the external signing step,
          |use submit instead."""
      )
      def prepare(
          actAs: Seq[Party],
          commands: Seq[Command],
          synchronizerId: Option[SynchronizerId] = None,
          commandId: String = UUID.randomUUID().toString,
          minLedgerTimeAbs: Option[Instant] = None,
          readAs: Seq[Party] = Seq.empty,
          disclosedContracts: Seq[DisclosedContract] = Seq.empty,
          userId: String = userId,
          userPackageSelectionPreference: Seq[LfPackageId] = Seq.empty,
          verboseHashing: Boolean = false,
          prefetchContractKeys: Seq[PrefetchContractKey] = Seq.empty,
      ): PrepareResponseProto =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.InteractiveSubmissionService.PrepareCommand(
              actAs.map(_.toLf),
              readAs.map(_.toLf),
              commands,
              commandId,
              minLedgerTimeAbs,
              disclosedContracts,
              synchronizerId,
              userId,
              userPackageSelectionPreference,
              verboseHashing,
              prefetchContractKeys,
            )
          )
        }

      @Help.Summary(
        "Execute a prepared submission"
      )
      @Help.Description(
        """
          preparedTransaction: the prepared transaction bytestring, typically obtained from the preparedTransaction field of the [[prepare]] response.
          transactionSignatures: the signatures of the hash of the transaction. The hash is typically obtained from the preparedTransactionHash field of the [[prepare]] response.
            Note however that the caller should re-compute the hash and ensure it matches the one provided in [[prepare]], to be certain they're signing a hash that correctly represents
            the transaction they want to submit.
          """
      )
      def execute(
          preparedTransaction: PreparedTransaction,
          transactionSignatures: Map[PartyId, Seq[Signature]],
          submissionId: String,
          hashingSchemeVersion: HashingSchemeVersion,
          userId: String = userId,
          deduplicationPeriod: Option[DeduplicationPeriod] = None,
          minLedgerTimeAbs: Option[Instant] = None,
      ): ExecuteResponseProto =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.InteractiveSubmissionService.ExecuteCommand(
              preparedTransaction,
              transactionSignatures,
              submissionId = submissionId,
              userId = userId,
              deduplicationPeriod = deduplicationPeriod,
              minLedgerTimeAbs = minLedgerTimeAbs,
              hashingSchemeVersion = hashingSchemeVersion,
            )
          )
        }

      @Help.Summary(
        "Execute a prepared submission and wait for it to complete (successfully or not)"
      )
      @Help.Description(
        """
          Similar to execute, except it will wait for the command to be completed before returning.
          Equivalent of "submitAndWait" in the CommandService.
          IMPORTANT: this command assumes that the executing participant is trusted to return a valid command completion.
          A dishonest executing participant could incorrectly respond that the command failed even though it succeeded.
          """
      )
      def execute_and_wait(
          preparedTransaction: PreparedTransaction,
          transactionSignatures: Map[PartyId, Seq[Signature]],
          submissionId: String,
          hashingSchemeVersion: HashingSchemeVersion,
          userId: String = userId,
          deduplicationPeriod: Option[DeduplicationPeriod] = None,
          minLedgerTimeAbs: Option[Instant] = None,
      ): ExecuteAndWaitResponseProto =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.InteractiveSubmissionService.ExecuteAndWaitCommand(
              preparedTransaction,
              transactionSignatures,
              submissionId = submissionId,
              userId = userId,
              deduplicationPeriod = deduplicationPeriod,
              minLedgerTimeAbs = minLedgerTimeAbs,
              hashingSchemeVersion = hashingSchemeVersion,
            )
          )
        }

      @Help.Summary(
        "Execute a prepared submission and return the resulting transaction"
      )
      @Help.Description(
        """
          Similar to executeAndWait, but returns the resulting transaction.
          IMPORTANT: this command assumes that the executing participant is trusted to return a valid command completion.
          A dishonest executing participant could incorrectly respond that the command failed even though it succeeded.
          """
      )
      def execute_and_wait_for_transaction(
          preparedTransaction: PreparedTransaction,
          transactionSignatures: Map[PartyId, Seq[Signature]],
          submissionId: String,
          hashingSchemeVersion: HashingSchemeVersion,
          transactionShape: Option[TransactionShape] = Some(TRANSACTION_SHAPE_LEDGER_EFFECTS),
          userId: String = userId,
          deduplicationPeriod: Option[DeduplicationPeriod] = None,
          minLedgerTimeAbs: Option[Instant] = None,
          includeCreatedEventBlob: Boolean = false,
      ): ApiTransaction =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.InteractiveSubmissionService.ExecuteAndWaitForTransactionCommand(
              preparedTransaction,
              transactionSignatures,
              submissionId = submissionId,
              userId = userId,
              deduplicationPeriod = deduplicationPeriod,
              minLedgerTimeAbs = minLedgerTimeAbs,
              hashingSchemeVersion = hashingSchemeVersion,
              transactionShape = transactionShape,
              includeCreatedEventBlob = includeCreatedEventBlob,
            )
          )
        }.getTransaction

      @Help.Summary("Get the preferred package version for constructing a command submission")
      @Help.Description(
        """A preferred package is the highest-versioned package for a provided package-name
           that is vetted by all the participants hosting the provided parties.
           Ledger API clients should use this endpoint for constructing command submissions
           that are compatible with the provided preferred package, by making informed decisions on:
             - which are the compatible packages that can be used to create contracts
             - which contract or exercise choice argument version can be used in the command
             - which choices can be executed on a template or interface of a contract
           parties: The parties whose vetting state should be considered when computing the preferred package
           packageName: The package name for which the preferred package is requested
           synchronizerId: The synchronizer whose topology state to use for resolving this query.
                           If not specified. the topology state of all the synchronizers the participant is connected to will be used.
           vettingValidAt: The timestamp at which the package vetting validity should be computed
                           If not provided, the participant's current clock time is used.
          """
      )
      def preferred_package_version(
          parties: Set[Party],
          packageName: LfPackageName,
          synchronizerId: Option[SynchronizerId] = None,
          vettingValidAt: Option[CantonTimestamp] = None,
      ): Option[PackagePreference] =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.InteractiveSubmissionService
              .PreferredPackageVersion(
                parties.map(_.toLf),
                packageName,
                synchronizerId,
                vettingValidAt,
              )
          )
        }

      @Help.Summary("Get the preferred packages for constructing a command submission")
      @Help.Description(
        """A preferred package is the highest-versioned package for a provided package-name
           that is vetted by all the participants hosting the provided parties.
           Ledger API clients should use this endpoint for constructing command submissions
           that are compatible with the provided preferred package, by making informed decisions on:
             - which are the compatible packages that can be used to create contracts
             - which contract or exercise choice argument version can be used in the command
             - which choices can be executed on a template or interface of a contract

           Generally it is enough to provide the requirements for the command's root package-names.
           Additional package-name requirements can be provided when additional informees need to use
           package dependencies of the command's root packages.

           parties: The parties whose vetting state should be considered when computing the preferred package
           packageName: The package name for which the preferred package is requested
           synchronizerId: The synchronizer whose topology state to use for resolving this query.
                           If not specified. the topology state of all the synchronizers the participant is connected to will be used.
           vettingValidAt: The timestamp at which the package vetting validity should be computed
                           If not provided, the participant's current clock time is used.
          """
      )
      def preferred_packages(
          packageVettingRequirements: Map[LfPackageName, Set[PartyId]],
          synchronizerId: Option[SynchronizerId] = None,
          vettingValidAt: Option[CantonTimestamp] = None,
      ): GetPreferredPackagesResponse =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.InteractiveSubmissionService
              .PreferredPackages(
                packageVettingRequirements.view.mapValues(_.map(_.toLf)).toMap,
                synchronizerId,
                vettingValidAt,
              )
          )
        }
    }

    @Help.Summary("Submit commands")
    @Help.Group("Command Submission")
    object commands extends Helpful {

      @Help.Summary(
        "Submit command and wait for the resulting transaction, returning the transaction or failing otherwise"
      )
      @Help.Description(
        """Submits a command on behalf of the `actAs` parties, waits for the resulting transaction to commit, and returns the "flattened" transaction.
          | If the timeout is set, it also waits for the transaction to appear at all other configured
          | participants who were involved in the transaction. The call blocks until the transaction commits or fails;
          | the timeout only specifies how long to wait at the other participants.
          | Fails if the transaction doesn't commit, or if it doesn't become visible to the involved participants in
          | the allotted time.
          | Note that if the optTimeout is set and the involved parties are concurrently enabled/disabled or their
          | participants are connected/disconnected, the command may currently result in spurious timeouts or may
          | return before the transaction appears at all the involved participants."""
      )
      def submit(
          actAs: Seq[Party],
          commands: Seq[Command],
          synchronizerId: Option[SynchronizerId] = None,
          workflowId: String = "",
          commandId: String = "",
          optTimeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
          deduplicationPeriod: Option[DeduplicationPeriod] = None,
          submissionId: String = "",
          minLedgerTimeAbs: Option[Instant] = None,
          readAs: Seq[Party] = Seq.empty,
          disclosedContracts: Seq[DisclosedContract] = Seq.empty,
          userId: String = userId,
          userPackageSelectionPreference: Seq[LfPackageId] = Seq.empty,
          transactionShape: TransactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          includeCreatedEventBlob: Boolean = false,
      ): ApiTransaction = {
        val externalParties = actAs.collect { case externalParty: ExternalParty => externalParty }

        // TODO(#27461) Support multiple submitting parties
        if (externalParties.sizeIs > 1)
          consoleEnvironment.raiseError(
            s"submit supports at most one external party, found: ${externalParties.map(_.partyId)}"
          )

        externalParties.headOption match {
          case Some(externalParty) =>
            external.submit(
              actAs = externalParty,
              commands,
              synchronizerId,
              commandId,
              optTimeout,
              deduplicationPeriod,
              submissionId,
              minLedgerTimeAbs,
              readAs,
              disclosedContracts,
              userId,
              userPackageSelectionPreference,
              /*
              TRANSACTION_SHAPE_ACS_DELTA implies that emitted events are only for locally hosted parties.
              In particular, it means that the "waiting" part of the submission fails if the executing participant
              does not host the party.
               */
              transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
            )

          case _ =>
            val tx = consoleEnvironment.run {
              ledgerApiCommand(
                LedgerApiCommands.CommandService.SubmitAndWaitTransaction(
                  actAs.map(_.toLf),
                  readAs.map(_.toLf),
                  commands,
                  workflowId,
                  commandId,
                  deduplicationPeriod,
                  submissionId,
                  minLedgerTimeAbs,
                  disclosedContracts,
                  synchronizerId,
                  userId,
                  userPackageSelectionPreference,
                  transactionShape,
                  includeCreatedEventBlob = includeCreatedEventBlob,
                )
              )
            }
            optionallyAwait(tx, tx.updateId, tx.synchronizerId, optTimeout)
        }
      }

      @Help.Summary("Submit command asynchronously")
      @Help.Description(
        """Provides access to the command submission service of the Ledger API.
          |See https://docs.daml.com/app-dev/services.html for documentation of the parameters."""
      )
      def submit_async(
          actAs: Seq[PartyId],
          commands: Seq[Command],
          synchronizerId: Option[SynchronizerId] = None,
          workflowId: String = "",
          commandId: String = "",
          deduplicationPeriod: Option[DeduplicationPeriod] = None,
          submissionId: String = "",
          minLedgerTimeAbs: Option[Instant] = None,
          readAs: Seq[Party] = Seq.empty,
          disclosedContracts: Seq[DisclosedContract] = Seq.empty,
          userId: String = userId,
          userPackageSelectionPreference: Seq[LfPackageId] = Seq.empty,
      ): Unit = consoleEnvironment.run {
        ledgerApiCommand(
          LedgerApiCommands.CommandSubmissionService.Submit(
            actAs.map(_.toLf),
            readAs.map(_.toLf),
            commands,
            workflowId,
            commandId,
            deduplicationPeriod,
            submissionId,
            minLedgerTimeAbs,
            disclosedContracts,
            synchronizerId,
            userId,
            userPackageSelectionPreference,
          )
        )
      }

      @Help.Summary("Investigate successful and failed commands")
      @Help.Description(
        """Find the status of commands. Note that only recent commands which are kept in memory will be returned."""
      )
      def status(
          commandIdPrefix: String = "",
          state: CommandState = CommandState.COMMAND_STATE_UNSPECIFIED,
          limit: PositiveInt = PositiveInt.tryCreate(10),
      ): Seq[CommandStatus] = check(FeatureFlag.Preview) {
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandInspectionService.GetCommandStatus(
              commandIdPrefix = commandIdPrefix,
              state = state,
              limit = limit.unwrap,
            )
          )
        }
      }

      @Help.Summary("Investigate failed commands")
      @Help.Description(
        """Same as status(..., state = CommandState.Failed)."""
      )
      def failed(commandId: String = "", limit: PositiveInt = PositiveInt.tryCreate(10)): Seq[
        CommandStatus
      ] = check(FeatureFlag.Preview) {
        status(commandId, CommandState.COMMAND_STATE_FAILED, limit)
      }

      @Help.Summary(
        "Submit assign command and wait for the resulting reassignment, returning the reassignment or failing otherwise"
      )
      @Help.Description(
        """Submits an assignment command on behalf of `submitter` party, waits for the resulting assignment to commit, and returns the reassignment.
          | If timeout is set, it also waits for the reassignment(s) to appear at all other configured
          | participants who were involved in the assignment. The call blocks until the assignment commits or fails.
          | Fails if the assignment doesn't commit, or if it doesn't become visible to the involved participants in time.
          | Timeout specifies the time how long to wait until the reassignment appears in the update stream for the submitting and all the involved participants.
          | The reassignmentId should be the one returned by the corresponding submit_unassign command."""
      )
      def submit_assign(
          submitter: PartyId,
          reassignmentId: String,
          source: SynchronizerId,
          target: SynchronizerId,
          workflowId: String = "",
          userId: String = userId,
          submissionId: String = UUID.randomUUID().toString,
          timeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
      ): AssignedWrapper =
        submit_assign_with_format(
          submitter = submitter,
          reassignmentId = reassignmentId,
          source = source,
          target = target,
          workflowId = workflowId,
          userId = userId,
          submissionId = submissionId,
          eventFormat = eventFormatAllParties(),
          timeout = timeout,
        ).assignedWrapper

      @Help.Summary(
        "Submit assign command and wait for the resulting reassignment, returning the reassignment or failing otherwise"
      )
      @Help.Description(
        """Submits an assignment command on behalf of `submitter` party, waits for the resulting assignment to commit, and returns the reassignment.
          | If timeout is set, it also waits for the reassignment(s) to appear at all other configured
          | participants who were involved in the assignment. The call blocks until the assignment commits or fails.
          | Fails if the assignment doesn't commit, or if it doesn't become visible to the involved participants in time.
          | Timeout specifies the time how long to wait until the reassignment appears in the update stream for the submitting and all the involved participants.
          | The reassignmentId should be the one returned by the corresponding submit_unassign command."""
      )
      def submit_assign_with_format(
          submitter: PartyId,
          reassignmentId: String,
          source: SynchronizerId,
          target: SynchronizerId,
          workflowId: String = "",
          userId: String = userId,
          submissionId: String = UUID.randomUUID().toString,
          eventFormat: Option[EventFormat],
          timeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
      ): EmptyOrAssignedWrapper = {
        val result = consoleEnvironment.run(
          ledgerApiCommand(
            LedgerApiCommands.CommandService.SubmitAndWaitAssign(
              submitter = submitter.toLf,
              reassignmentId = reassignmentId,
              source = source,
              target = target,
              workflowId = workflowId,
              userId = userId,
              commandId = UUID.randomUUID().toString,
              submissionId = submissionId,
              eventFormat = eventFormat,
            )
          )
        )
        optionallyAwait(result, result.updateId, target.toProtoPrimitive, timeout)
      }

      @Help.Summary(
        "Submit unassign command and wait for the resulting reassignment, returning the reassignment or failing otherwise"
      )
      @Help.Description(
        """Submits an unassignment command on behalf of `submitter` party, waits for the resulting unassignment to commit, and returns the reassignment.
          | If timeout is set, it also waits for the reassignment(s) to appear at all other configured
          | participants who were involved in the unassignment. The call blocks until the unassignment commits or fails.
          | Fails if the unassignment doesn't commit, or if it doesn't become visible to the involved participants in time.
          | Timeout specifies the time how long to wait until the reassignment appears in the update stream for the submitting and all the involved participants."""
      )
      def submit_unassign(
          submitter: PartyId,
          contractIds: Seq[LfContractId],
          source: SynchronizerId,
          target: SynchronizerId,
          workflowId: String = "",
          userId: String = userId,
          submissionId: String = UUID.randomUUID().toString,
          timeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
      ): UnassignedWrapper =
        submit_unassign_with_format(
          submitter = submitter,
          contractIds = contractIds,
          source = source,
          target = target,
          workflowId = workflowId,
          userId = userId,
          submissionId = submissionId,
          eventFormat = eventFormatAllParties(),
          timeout = timeout,
        ).unassignedWrapper

      @Help.Summary(
        "Submit unassign command and wait for the resulting reassignment, returning the reassignment or failing otherwise"
      )
      @Help.Description(
        """Submits an unassignment command on behalf of `submitter` party, waits for the resulting unassignment to commit, and returns the reassignment.
          | If timeout is set, it also waits for the reassignment(s) to appear at all other configured
          | participants who were involved in the unassignment. The call blocks until the unassignment commits or fails.
          | Fails if the unassignment doesn't commit, or if it doesn't become visible to the involved participants in time.
          | Timeout specifies the time how long to wait until the reassignment appears in the update stream for the submitting and all the involved participants."""
      )
      def submit_unassign_with_format(
          submitter: PartyId,
          contractIds: Seq[LfContractId],
          source: SynchronizerId,
          target: SynchronizerId,
          workflowId: String = "",
          userId: String = userId,
          submissionId: String = UUID.randomUUID().toString,
          eventFormat: Option[EventFormat],
          timeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
      ): EmptyOrUnassignedWrapper = {
        val result = consoleEnvironment.run(
          ledgerApiCommand(
            LedgerApiCommands.CommandService.SubmitAndWaitUnassign(
              submitter = submitter.toLf,
              contractIds = contractIds,
              source = source,
              target = target,
              workflowId = workflowId,
              userId = userId,
              commandId = UUID.randomUUID().toString,
              submissionId = submissionId,
              eventFormat = eventFormat,
            )
          )
        )
        optionallyAwait(result, result.updateId, source.toProtoPrimitive, timeout)
      }

      @Help.Summary("Combines `submit_unassign` and `submit_assign` in a single macro")
      @Help.Description(
        """See `submit_unassign` and `submit_assign` for the parameters."""
      )
      def submit_reassign(
          submitter: PartyId,
          contractIds: Seq[LfContractId],
          source: SynchronizerId,
          target: SynchronizerId,
          workflowId: String = "",
          userId: String = userId,
          submissionId: String = UUID.randomUUID().toString,
          timeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
      ): (UnassignedWrapper, AssignedWrapper) = {
        val unassigned = submit_unassign(
          submitter,
          contractIds,
          source,
          target,
          workflowId,
          userId,
          submissionId,
          timeout,
        )
        val assigned = submit_assign(
          submitter,
          unassigned.reassignmentId,
          source,
          target,
          workflowId,
          userId,
          submissionId,
          timeout,
        )
        (unassigned, assigned)
      }

      @Help.Summary("Submit assign command asynchronously")
      @Help.Description(
        """Provides access to the command submission service of the Ledger API.
          |See https://docs.daml.com/app-dev/services.html for documentation of the parameters."""
      )
      def submit_assign_async(
          submitter: PartyId,
          reassignmentId: String,
          source: SynchronizerId,
          target: SynchronizerId,
          workflowId: String = "",
          userId: String = userId,
          commandId: String = UUID.randomUUID().toString,
          submissionId: String = UUID.randomUUID().toString,
      ): Unit =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandSubmissionService.SubmitAssignCommand(
              workflowId = workflowId,
              userId = userId,
              commandId = commandId,
              submitter = submitter.toLf,
              submissionId = submissionId,
              reassignmentId = reassignmentId,
              source = source,
              target = target,
            )
          )
        }

      @Help.Summary("Submit unassign command asynchronously")
      @Help.Description(
        """Provides access to the command submission service of the Ledger API.
          |See https://docs.daml.com/app-dev/services.html for documentation of the parameters."""
      )
      def submit_unassign_async(
          submitter: PartyId,
          contractIds: Seq[LfContractId],
          source: SynchronizerId,
          target: SynchronizerId,
          workflowId: String = "",
          userId: String = userId,
          commandId: String = UUID.randomUUID().toString,
          submissionId: String = UUID.randomUUID().toString,
      ): Unit =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandSubmissionService.SubmitUnassignCommand(
              workflowId = workflowId,
              userId = userId,
              commandId = commandId,
              submitter = submitter.toLf,
              submissionId = submissionId,
              contractIds = contractIds,
              source = source,
              target = target,
            )
          )
        }

      @Help.Summary("Submit commands on behalf of external parties")
      @Help.Group("Command Submission")
      private[canton] object external {
        def submit(
            actAs: ExternalParty, // TODO(#27461) Support multiple submitting parties
            commands: Seq[Command],
            synchronizerId: Option[SynchronizerId] = None,
            commandId: String = "",
            optTimeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
            deduplicationPeriod: Option[DeduplicationPeriod] = None,
            submissionId: String = "",
            minLedgerTimeAbs: Option[Instant] = None,
            readAs: Seq[Party] = Seq.empty,
            disclosedContracts: Seq[DisclosedContract] = Seq.empty,
            userId: String = userId,
            userPackageSelectionPreference: Seq[LfPackageId] = Seq.empty,
            transactionShape: TransactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
            includeCreatedEventBlob: Boolean = false,
            // External party specifics
            verboseHashing: Boolean = false,
        ): ApiTransaction = {

          val prepared = ledger_api.interactive_submission.prepare(
            actAs = Seq(actAs.partyId),
            commands = commands,
            synchronizerId = synchronizerId,
            commandId = if (commandId.isEmpty) UUID.randomUUID().toString else commandId,
            minLedgerTimeAbs = minLedgerTimeAbs,
            readAs = readAs,
            disclosedContracts = disclosedContracts,
            userId = userId,
            userPackageSelectionPreference = userPackageSelectionPreference,
            verboseHashing = verboseHashing,
            prefetchContractKeys = Seq(),
          )

          submit_prepared(
            preparedTransaction = prepared,
            actAs = actAs,
            optTimeout = optTimeout,
            deduplicationPeriod = deduplicationPeriod,
            submissionId = submissionId,
            minLedgerTimeAbs = minLedgerTimeAbs,
            userId = userId,
            transactionShape = transactionShape,
            includeCreatedEventBlob = includeCreatedEventBlob,
          )
        }

        def submit_prepared(
            actAs: ExternalParty, // TODO(#27461) Support multiple submitting parties
            preparedTransaction: PrepareResponseProto,
            optTimeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
            deduplicationPeriod: Option[DeduplicationPeriod] = None,
            submissionId: String = "",
            minLedgerTimeAbs: Option[Instant] = None,
            userId: String = userId,
            transactionShape: TransactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
            includeCreatedEventBlob: Boolean = false,
        ): ApiTransaction = {

          val prepared = preparedTransaction.preparedTransaction.getOrElse(
            consoleEnvironment.raiseError("Prepared transaction was empty")
          )

          val signatures = Map(
            actAs.partyId -> consoleEnvironment.global_secret
              .sign(preparedTransaction.preparedTransactionHash, actAs)
          )

          val tx = ledger_api.interactive_submission
            .execute_and_wait_for_transaction(
              preparedTransaction = prepared,
              transactionSignatures = signatures,
              submissionId = submissionId,
              hashingSchemeVersion = preparedTransaction.hashingSchemeVersion,
              transactionShape = Some(transactionShape),
              userId = userId,
              deduplicationPeriod = deduplicationPeriod,
              minLedgerTimeAbs = minLedgerTimeAbs,
              includeCreatedEventBlob = includeCreatedEventBlob,
            )

          optionallyAwait(tx, tx.updateId, tx.synchronizerId, optTimeout)
        }
      }
    }

    @Help.Summary("Collection of Ledger API state endpoints")
    @Help.Group("State")
    object state extends Helpful {

      @Help.Summary("Read the current ledger end offset")
      def end(): Long =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.StateService.LedgerEnd()
          )
        }

      @Help.Summary("Read the current connected synchronizers for a party", FeatureFlag.Testing)
      def connected_synchronizers(partyId: Party): GetConnectedSynchronizersResponse =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.StateService.GetConnectedSynchronizers(partyId.toLf)
          )
        })

      @Help.Summary("Investigate successful and failed commands", FeatureFlag.Testing)
      @Help.Description(
        """Find the status of commands. Note that only recent commands which are kept in memory will be returned."""
      )
      def status(
          commandIdPrefix: String = "",
          state: CommandState = CommandState.COMMAND_STATE_UNSPECIFIED,
          limit: PositiveInt = PositiveInt.tryCreate(10),
      ): Seq[CommandStatus] = check(FeatureFlag.Preview) {
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandInspectionService.GetCommandStatus(
              commandIdPrefix = commandIdPrefix,
              state = state,
              limit = limit.unwrap,
            )
          )
        }
      }

      @Help.Summary("Investigate failed commands", FeatureFlag.Testing)
      @Help.Description(
        """Same as status(..., state = CommandState.Failed)."""
      )
      def failed(commandId: String = "", limit: PositiveInt = PositiveInt.tryCreate(10)): Seq[
        CommandStatus
      ] = check(FeatureFlag.Preview) {
        status(commandId, CommandState.COMMAND_STATE_FAILED, limit)
      }

      @Help.Summary("Read active contracts")
      @Help.Group("Active Contracts")
      object acs extends Helpful {
        @Help.Summary("List the set of active contract entries of a given party")
        @Help.Description(
          """This command will return the current set of active contracts and incomplete reassignments for the given party.
            |
            |Supported arguments:
            |- party: for which party you want to load the acs
            |- limit: limit (default set via canton.parameter.console)
            |- verbose: whether the resulting events should contain detailed type information
            |- filterTemplate: list of templates ids to filter for, empty sequence acts as a wildcard
            |- activeAtOffsetO: the offset at which the snapshot of the active contracts will be computed, it
            |  must be no greater than the current ledger end offset and must be greater than or equal to the
            |  last pruning offset. If no offset is specified then the current participant end will be used.
            |- timeout: the maximum wait time for the complete acs to arrive
            |- includeCreatedEventBlob: whether the result should contain the createdEventBlobs, it works only
            |  if the filterTemplate is non-empty
            |- resultFilter: custom filter of the results, applies before limit"""
        )
        def of_party(
            party: Party,
            limit: PositiveInt = defaultLimit,
            verbose: Boolean = true,
            filterTemplates: Seq[TemplateId] = Seq.empty,
            activeAtOffsetO: Option[Long] = None,
            timeout: config.NonNegativeDuration = timeouts.unbounded,
            includeCreatedEventBlob: Boolean = false,
            resultFilter: GetActiveContractsResponse => Boolean = _.contractEntry.isDefined,
        ): Seq[WrappedContractEntry] = {
          val observer =
            new RecordingStreamObserver[GetActiveContractsResponse](limit, resultFilter)
          val activeAt =
            activeAtOffsetO match {
              case None =>
                consoleEnvironment.run {
                  ledgerApiCommand(
                    LedgerApiCommands.StateService.LedgerEnd()
                  )
                }
              case Some(offset) => offset
            }
          mkResult(
            consoleEnvironment.run {
              ledgerApiCommand(
                LedgerApiCommands.StateService.GetActiveContracts(
                  observer,
                  Set(party.toLf),
                  limit,
                  filterTemplates,
                  activeAt,
                  verbose,
                  timeout.asFiniteApproximation,
                  includeCreatedEventBlob,
                )
              )
            },
            "getActiveContracts",
            observer,
            timeout,
          ).map(activeContract => WrappedContractEntry(activeContract.contractEntry))
        }

        @Help.Summary("List the set of active contracts of a given party")
        @Help.Description(
          """This command will return the current set of active contracts for the given party.
            |
            |Supported arguments:
            |- party: for which party you want to load the acs
            |- limit: limit (default set via canton.parameter.console)
            |- verbose: whether the resulting events should contain detailed type information
            |- filterTemplate: list of templates ids to filter for, empty sequence acts as a wildcard
            |- activeAtOffsetO: the offset at which the snapshot of the active contracts will be computed, it
            |  must be no greater than the current ledger end offset and must be greater than or equal to the
            |  last pruning offset. If no offset is specified then the current participant end will be used.
            |- timeout: the maximum wait time for the complete acs to arrive
            |- includeCreatedEventBlob: whether the result should contain the createdEventBlobs, it works only
            |  if the filterTemplate is non-empty"""
        )
        def active_contracts_of_party(
            party: Party,
            limit: PositiveInt = defaultLimit,
            verbose: Boolean = true,
            filterTemplates: Seq[TemplateId] = Seq.empty,
            activeAtOffsetO: Option[Long] = None,
            timeout: config.NonNegativeDuration = timeouts.unbounded,
            includeCreatedEventBlob: Boolean = false,
        ): Seq[ActiveContract] =
          of_party(
            party.partyId,
            limit,
            verbose,
            filterTemplates,
            activeAtOffsetO,
            timeout,
            includeCreatedEventBlob,
            _.contractEntry.isActiveContract,
          )
            .flatMap(_.entry.activeContract)

        @Help.Summary("List the set of incomplete unassigned events of a given party")
        @Help.Description(
          """This command will return the current set of incomplete unassigned events for the given party.
            |
            |Supported arguments:
            |- party: for which party you want to load the acs
            |- limit: limit (default set via canton.parameter.console)
            |- verbose: whether the resulting events should contain detailed type information
            |- filterTemplate: list of templates ids to filter for, empty sequence acts as a wildcard
            |- activeAtOffsetO: the offset at which the snapshot of the events will be computed, it
            |  must be no greater than the current ledger end offset and must be greater than or equal to the
            |  last pruning offset. If no offset is specified then the current participant end will be used.
            |- timeout: the maximum wait time for the complete acs to arrive
            |- includeCreatedEventBlob: whether the result should contain the createdEventBlobs, it works only
            |  if the filterTemplate is non-empty"""
        )
        def incomplete_unassigned_of_party(
            party: Party,
            limit: PositiveInt = defaultLimit,
            verbose: Boolean = true,
            filterTemplates: Seq[TemplateId] = Seq.empty,
            activeAtOffsetO: Option[Long] = None,
            timeout: config.NonNegativeDuration = timeouts.unbounded,
            includeCreatedEventBlob: Boolean = false,
        ): Seq[WrappedIncompleteUnassigned] =
          of_party(
            party,
            limit,
            verbose,
            filterTemplates,
            activeAtOffsetO,
            timeout,
            includeCreatedEventBlob,
            _.contractEntry.isIncompleteUnassigned,
          )
            .flatMap(_.entry.incompleteUnassigned)
            .map(WrappedIncompleteUnassigned(_))

        @Help.Summary("List the set of incomplete assigned events of a given party")
        @Help.Description(
          """This command will return the current set of incomplete assigned events for the given party.
            |
            |Supported arguments:
            |- party: for which party you want to load the acs
            |- limit: limit (default set via canton.parameter.console)
            |- verbose: whether the resulting events should contain detailed type information
            |- filterTemplate: list of templates ids to filter for, empty sequence acts as a wildcard
            |- activeAtOffsetO: the offset at which the snapshot of the events will be computed, it must be no
            |  greater than the current ledger end offset and must be greater than or equal to the last
            |  pruning offset. If no offset is specified then the current participant end will be used.
            |- timeout: the maximum wait time for the complete acs to arrive
            |- includeCreatedEventBlob: whether the result should contain the createdEventBlobs, it works only
            |  if the filterTemplate is non-empty"""
        )
        def incomplete_assigned_of_party(
            party: Party,
            limit: PositiveInt = defaultLimit,
            verbose: Boolean = true,
            filterTemplates: Seq[TemplateId] = Seq.empty,
            activeAtOffsetO: Option[Long] = None,
            timeout: config.NonNegativeDuration = timeouts.unbounded,
            includeCreatedEventBlob: Boolean = false,
        ): Seq[WrappedIncompleteAssigned] =
          of_party(
            party,
            limit,
            verbose,
            filterTemplates,
            activeAtOffsetO,
            timeout,
            includeCreatedEventBlob,
            _.contractEntry.isIncompleteAssigned,
          )
            .flatMap(_.entry.incompleteAssigned)
            .map(WrappedIncompleteAssigned(_))

        @Help.Summary(
          "List the set of active contracts for all parties hosted on this participant"
        )
        @Help.Description(
          """This command will return the current set of active contracts for all parties.

             Supported arguments:
             - limit: limit (default set via canton.parameter.console)
             - verbose: whether the resulting events should contain detailed type information
             - filterTemplate: list of templates ids to filter for, empty sequence acts as a wildcard
             - activeAtOffsetO: the offset at which the snapshot of the active contracts will be computed, it
               must be no greater than the current ledger end offset and must be greater than or equal to the
               last pruning offset. If no offset is specified then the current participant end will be used.
             - timeout: the maximum wait time for the complete acs to arrive
             - identityProviderId: limit the response to parties governed by the given identity provider
             - includeCreatedEventBlob: whether the result should contain the createdEventBlobs, it works only
               if the filterTemplate is non-empty
             - resultFilter: custom filter of the results, applies before limit
          """
        )
        def of_all(
            limit: PositiveInt = defaultLimit,
            verbose: Boolean = true,
            filterTemplates: Seq[TemplateId] = Seq.empty,
            activeAtOffsetO: Option[Long] = None,
            timeout: config.NonNegativeDuration = timeouts.unbounded,
            identityProviderId: String = "",
            includeCreatedEventBlob: Boolean = false,
            resultFilter: GetActiveContractsResponse => Boolean = _.contractEntry.isDefined,
        ): Seq[WrappedContractEntry] =
          consoleEnvironment.runE {
            for {
              parties <- ledgerApiCommand(
                LedgerApiCommands.PartyManagementService.ListKnownParties(
                  identityProviderId = identityProviderId
                )
              ).toEither
              localParties <- parties.filter(_.isLocal).map(_.party).traverse(LfPartyId.fromString)
              res <- {
                if (localParties.isEmpty) Right(Seq.empty)
                else {
                  val observer = new RecordingStreamObserver[GetActiveContractsResponse](
                    limit,
                    resultFilter,
                  )
                  Try(
                    mkResult(
                      consoleEnvironment.run {
                        ledgerApiCommand(
                          LedgerApiCommands.StateService.GetActiveContracts(
                            observer,
                            localParties.toSet,
                            limit,
                            filterTemplates,
                            activeAtOffsetO.getOrElse(end()),
                            verbose,
                            timeout.asFiniteApproximation,
                            includeCreatedEventBlob,
                          )
                        )
                      },
                      "getActiveContracts",
                      observer,
                      timeout,
                    ).map(activeContract => WrappedContractEntry(activeContract.contractEntry))
                  ).toEither.left.map(_.getMessage)
                }
              }
            } yield res
          }

        @Help.Summary(
          "Wait until the party sees the given contract in the active contract service"
        )
        @Help.Description(
          "Will throw an exception if the contract is not found to be active within the given timeout"
        )
        def await_active_contract(
            party: Party,
            contractId: LfContractId,
            timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
        ): Unit =
          ConsoleMacros.utils.retry_until_true(timeout) {
            of_party(party, verbose = false)
              .exists(_.contractId == contractId.coid)
          }

        @Help.Summary("Generic search for contracts")
        @Help.Description(
          """This search function returns an untyped ledger-api event.
            |The find will wait until the contract appears or throw an exception once it times out."""
        )
        def find_generic(
            partyId: Party,
            filter: WrappedContractEntry => Boolean,
            timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
        ): WrappedContractEntry = {
          def scan: Option[WrappedContractEntry] = of_party(partyId).find(filter(_))

          ConsoleMacros.utils.retry_until_true(timeout)(scan.isDefined)
          consoleEnvironment.runE {
            scan.toRight(s"Failed to find contract for $partyId.")
          }
        }
      }
    }

    @Help.Summary("Manage parties through the Ledger API")
    @Help.Group("Party Management")
    object parties extends Helpful {

      // TODO(i26846): document the userId parameter here and in the parties.rst documentation.
      @Help.Summary("Allocate a new party")
      @Help.Description(
        """Allocates a new party on the ledger.
          party: a hint for generating the party identifier
          annotations: key-value pairs associated with this party and stored locally on this Ledger API server
          identityProviderId: identity provider id
          synchronizerId: The synchronizer on which the party should be allocated.
                          The participant must be connected to the synchronizer.
                          The parameter may be omitted if the participant is connected to only one synchronizer."""
      )
      def allocate(
          party: String,
          annotations: Map[String, String] = Map.empty,
          identityProviderId: String = "",
          synchronizerId: Option[SynchronizerId] = None,
          userId: String = "",
      ): PartyDetails = {
        val proto = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.PartyManagementService.AllocateParty(
              partyIdHint = party,
              annotations = annotations,
              identityProviderId = identityProviderId,
              synchronizerId = synchronizerId,
              userId = userId,
            )
          )
        }

        PartyDetails.fromProtoPartyDetails(proto)
      }

      @Help.Summary("List parties known by the Ledger API server")
      @Help.Description(
        """Lists parties known by the Ledger API server.
           identityProviderId: identity provider id"""
      )
      def list(identityProviderId: String = ""): Seq[PartyDetails] = {
        val proto = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.PartyManagementService.ListKnownParties(
              identityProviderId = identityProviderId
            )
          )
        }

        proto.map(PartyDetails.fromProtoPartyDetails)
      }

      @Help.Summary("Update participant-local party details")
      @Help.Description(
        """Currently you can update only the annotations.
           |You cannot update other user attributes.
          party: party to be updated,
          modifier: a function to modify the party details, e.g.: `partyDetails => { partyDetails.copy(annotations = partyDetails.annotations.updated("a", "b").removed("c")) }`
          identityProviderId: identity provider id"""
      )
      def update(
          party: Party,
          modifier: PartyDetails => PartyDetails,
          identityProviderId: String = "",
      ): PartyDetails = {
        val rawDetails = get(party = party)
        val srcDetails = PartyDetails.fromProtoPartyDetails(rawDetails)
        val modifiedDetails = modifier(srcDetails)
        verifyOnlyModifiableFieldsWhereModified(srcDetails, modifiedDetails)
        val annotationsUpdate = makeAnnotationsUpdate(
          original = srcDetails.annotations,
          modified = modifiedDetails.annotations,
        )
        val rawUpdatedDetails = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.PartyManagementService.Update(
              party = party,
              annotationsUpdate = Some(annotationsUpdate),
              resourceVersionO = Some(rawDetails.localMetadata.fold("")(_.resourceVersion)),
              identityProviderId = identityProviderId,
            )
          )
        }

        PartyDetails.fromProtoPartyDetails(rawUpdatedDetails)
      }

      @Help.Summary("Update party's identity provider id")
      @Help.Description(
        """Updates party's identity provider id.
          party: party to be updated
          sourceIdentityProviderId: source identity provider id
          targetIdentityProviderId: target identity provider id
          """
      )
      def update_idp(
          party: PartyId,
          sourceIdentityProviderId: String,
          targetIdentityProviderId: String,
      ): Unit = consoleEnvironment.run {
        ledgerApiCommand(
          LedgerApiCommands.PartyManagementService.UpdateIdp(
            party = party,
            sourceIdentityProviderId = sourceIdentityProviderId,
            targetIdentityProviderId = targetIdentityProviderId,
          )
        )
      }

      private def verifyOnlyModifiableFieldsWhereModified(
          srcDetails: PartyDetails,
          modifiedDetails: PartyDetails,
      ): Unit = {
        val withAllowedUpdatesReverted = modifiedDetails.copy(annotations = srcDetails.annotations)
        if (withAllowedUpdatesReverted != srcDetails) {
          throw ModifyingNonModifiablePartyDetailsPropertiesError()
        }
      }

      private def get(party: Party, identityProviderId: String = ""): ProtoPartyDetails =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.PartyManagementService.GetParty(
              party = party,
              identityProviderId = identityProviderId,
            )
          )
        }
    }

    @Help.Summary("Manage packages")
    @Help.Group("Package Management")
    object packages extends Helpful {

      @Help.Summary("Upload packages from Dar file")
      @Help.Description("""Uploading the Dar can be done either through the ledger Api server or through the Canton admin Api.
          |The Ledger Api is the portable method across ledgers. The Canton admin Api is more powerful as it allows for
          |controlling Canton specific behaviour.
          |In particular, a Dar uploaded using the ledger Api will not be available in the Dar store and can not be downloaded again.
          |Additionally, Dars uploaded using the ledger Api will be vetted, but the system will not wait
          |for the Dars to be successfully registered with all connected synchronizers. As such, if a Dar is uploaded and then
          |used immediately thereafter, a command might bounce due to missing package vettings.""")
      def upload_dar(darPath: String): Unit =
        consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.PackageManagementService.UploadDarFile(darPath))
        }

      @Help.Summary("List Daml Packages")
      def list(limit: PositiveInt = defaultLimit): Seq[PackageDetails] =
        consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.PackageManagementService.ListKnownPackages(limit))
        }

      @Help.Summary("Validate a DAR against the current participants' state")
      @Help.Description(
        """Performs the same DAR and Daml package validation checks that the upload call performs,
         but with no effects on the target participants: the DAR is not persisted or vetted."""
      )
      def validate_dar(darPath: String): Unit =
        consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.PackageManagementService.ValidateDarFile(darPath))
        }
    }

    @Help.Summary("Monitor progress of commands")
    @Help.Group("Command Completions")
    object completions extends Helpful {

      @Help.Summary("Lists command completions following the specified offset")
      @Help.Description(
        """If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than
          |the pruning offset, this command fails with a `NOT_FOUND` error.
          |An empty offset denotes the beginning of the participant's offsets."""
      )
      def list(
          partyId: Party,
          atLeastNumCompletions: Int,
          beginOffsetExclusive: Long,
          userId: String = userId,
          timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
          filter: Completion => Boolean = _ => true,
      ): Seq[Completion] = consoleEnvironment.run {
        ledgerApiCommand(
          LedgerApiCommands.CommandCompletionService.CompletionRequest(
            partyId.toLf,
            beginOffsetExclusive,
            atLeastNumCompletions,
            timeout.asJavaApproximation,
            userId,
          )(filter, consoleEnvironment.environment.scheduler)
        )
      }

      @Help.Summary("Subscribe to the command completion stream")
      @Help.Description(
        """This function connects to the command completion stream and passes command completions to `observer` until
          |the stream is completed.
          |Only completions for parties in `parties` will be returned.
          |The returned completions start at `beginOffset` (default: the zero value denoting the participant begin).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error."""
      )
      def subscribe(
          observer: StreamObserver[Completion],
          parties: Seq[Party],
          beginOffsetExclusive: Long = 0L,
          userId: String = userId,
      ): AutoCloseable =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandCompletionService.Subscribe(
              observer,
              parties.map(_.toLf),
              beginOffsetExclusive,
              userId,
            )
          )
        }
    }

    @Help.Summary("Identity Provider Configuration Management")
    @Help.Group("Ledger Api Identity Provider Configuration Management")
    object identity_provider_config extends Helpful {
      @Help.Summary("Create a new identity provider configuration")
      @Help.Description(
        """Create an identity provider configuration. The request will fail if the maximum allowed number of separate configurations is reached."""
      )
      def create(
          identityProviderId: String,
          isDeactivated: Boolean = false,
          jwksUrl: String,
          issuer: String,
          audience: Option[String],
      ): IdentityProviderConfig = {
        val config = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.IdentityProviderConfigs.Create(
              identityProviderId =
                IdentityProviderId.Id(Ref.LedgerString.assertFromString(identityProviderId)),
              isDeactivated = isDeactivated,
              jwksUrl = JwksUrl.assertFromString(jwksUrl),
              issuer = issuer,
              audience = audience,
            )
          )
        }

        IdentityProviderConfigClient.fromProtoConfig(config)
      }

      @Help.Summary("Update an identity provider")
      @Help.Description("""Update identity provider""")
      def update(
          identityProviderId: String,
          isDeactivated: Boolean = false,
          jwksUrl: String,
          issuer: String,
          audience: Option[String],
          updateMask: FieldMask,
      ): IdentityProviderConfig = {
        val config = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.IdentityProviderConfigs.Update(
              IdentityProviderConfig(
                IdentityProviderId.Id(Ref.LedgerString.assertFromString(identityProviderId)),
                isDeactivated,
                JwksUrl(jwksUrl),
                issuer,
                audience,
              ),
              updateMask,
            )
          )
        }

        IdentityProviderConfigClient.fromProtoConfig(config)
      }

      @Help.Summary("Delete an identity provider configuration")
      @Help.Description("""Delete an existing identity provider configuration""")
      def delete(identityProviderId: String): Unit =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.IdentityProviderConfigs.Delete(
              IdentityProviderId.Id(Ref.LedgerString.assertFromString(identityProviderId))
            )
          )
        }

      @Help.Summary("Get an identity provider configuration")
      @Help.Description("""Get identity provider configuration by id""")
      def get(identityProviderId: String): IdentityProviderConfig = {
        val config = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.IdentityProviderConfigs.Get(
              IdentityProviderId.Id(Ref.LedgerString.assertFromString(identityProviderId))
            )
          )
        }
        IdentityProviderConfigClient.fromProtoConfig(config)
      }

      @Help.Summary("List identity provider configurations")
      @Help.Description("""List all existing identity provider configurations""")
      def list(): Seq[IdentityProviderConfig] = {
        val configs = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.IdentityProviderConfigs.List()
          )
        }
        configs.map(IdentityProviderConfigClient.fromProtoConfig)
      }
    }

    @Help.Summary("Manage Ledger Api Users")
    @Help.Group("Ledger Api Users")
    object users extends Helpful {

      @Help.Summary("Create a user with the given id")
      @Help.Description(
        """Users are used to dynamically managing the rights given to Daml users.
          |They allow us to link a stable local identifier (of an application) with a set of parties.
          id: the id used to identify the given user
          actAs: the set of parties this user is allowed to act as
          primaryParty: the optional party that should be linked to this user by default
          readAs: the set of parties this user is allowed to read as
          participantAdmin: flag (default false) indicating if the user is allowed to use the admin commands of the Ledger Api
          identityProviderAdmin: flag (default false) indicating if the user is allowed to manage users and parties assigned to the same identity provider
          isDeactivated: flag (default false) indicating if the user is active
          annotations: the set of key-value pairs linked to this user
          identityProviderId: identity provider id
          readAsAnyParty: flag (default false) indicating if the user is allowed to read as any party
          executeAs: the set of parties for whom this user is allowed to operate interactive submissions
          executeAsAnyParty: flag (default false) indicating if the user is allowed to operate interactive submissions as any party
          """
      )
      def create(
          id: String,
          actAs: Set[PartyId] = Set(),
          primaryParty: Option[PartyId] = None,
          readAs: Set[PartyId] = Set(),
          participantAdmin: Boolean = false,
          identityProviderAdmin: Boolean = false,
          isDeactivated: Boolean = false,
          annotations: Map[String, String] = Map.empty,
          identityProviderId: String = "",
          readAsAnyParty: Boolean = false,
          executeAs: Set[PartyId] = Set(),
          executeAsAnyParty: Boolean = false,
      ): User = {
        val lapiUser = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Users.Create(
              id = id,
              actAs = actAs.map(_.toLf),
              primaryParty = primaryParty.map(_.toLf),
              readAs = readAs.map(_.toLf),
              participantAdmin = participantAdmin,
              identityProviderAdmin = identityProviderAdmin,
              isDeactivated = isDeactivated,
              annotations = annotations,
              identityProviderId = identityProviderId,
              readAsAnyParty = readAsAnyParty,
              executeAs = executeAs.map(_.toLf),
              executeAsAnyParty = executeAsAnyParty,
            )
          )
        }

        User.fromLapiUser(lapiUser)
      }

      @Help.Summary("Update a user")
      @Help.Description(
        """Currently you can update the annotations, active status and primary party.
          |You cannot update other user attributes.
          id: id of the user to be updated
          modifier: a function for modifying the user; e.g: `user => { user.copy(isActive = false, primaryParty = None, annotations = user.annotations.updated("a", "b").removed("c")) }`
          identityProviderId: identity provider id
          """
      )
      def update(
          id: String,
          modifier: User => User,
          identityProviderId: String = "",
      ): User = {
        val rawUser = doGet(
          id = id,
          identityProviderId = identityProviderId,
        )
        val srcUser = User.fromLapiUser(rawUser)
        val modifiedUser = modifier(srcUser)
        verifyOnlyModifiableFieldsWhereModified(srcUser, modifiedUser)
        val annotationsUpdate =
          makeAnnotationsUpdate(original = srcUser.annotations, modified = modifiedUser.annotations)
        val rawUpdatedUser = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Users.Update(
              id = id,
              annotationsUpdate = Some(annotationsUpdate),
              primaryPartyUpdate = Some(modifiedUser.primaryParty),
              isDeactivatedUpdate = Some(modifiedUser.isDeactivated),
              resourceVersionO = Some(rawUser.metadata.resourceVersion),
              identityProviderId = identityProviderId,
            )
          )
        }

        User.fromLapiUser(rawUpdatedUser)
      }

      @Help.Summary("Get the user data of the user with the given id")
      @Help.Description(
        """Fetch the data associated with the given user id failing if there is no such user.
          |You will get the user's primary party, active status and annotations.
          |If you need the user rights, use rights.list instead.
          id: user id
          identityProviderId: identity provider id"""
      )
      def get(id: String, identityProviderId: String = ""): User = User.fromLapiUser(
        doGet(
          id = id,
          identityProviderId = identityProviderId,
        )
      )

      @Help.Summary("Delete a user")
      @Help.Description("""Delete a user by id.
         id: user id
         identityProviderId: identity provider id""")
      def delete(id: String, identityProviderId: String = ""): Unit =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Users.Delete(
              id = id,
              identityProviderId = identityProviderId,
            )
          )
        }

      @Help.Summary("List users")
      @Help.Description("""List users of this participant node
          filterUser: filter results using the given filter string
          pageToken: used for pagination (the result contains a page token if there are further pages)
          pageSize: default page size before the filter is applied
          identityProviderId: identity provider id""")
      def list(
          filterUser: String = "",
          pageToken: String = "",
          pageSize: Int = 100,
          identityProviderId: String = "",
      ): UsersPage = {
        val page: ListLedgerApiUsersResult = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Users.List(
              filterUser = filterUser,
              pageToken = pageToken,
              pageSize = pageSize,
              identityProviderId = identityProviderId,
            )
          )
        }

        UsersPage(
          users = page.users.map(User.fromLapiUser),
          nextPageToken = page.nextPageToken,
        )
      }

      @Help.Summary("Update user's identity provider id")
      @Help.Description(
        """Updates user's identity provider id.
          id: the id used to identify the given user
          sourceIdentityProviderId: source identity provider id
          targetIdentityProviderId: target identity provider id
          """
      )
      def update_idp(
          id: String,
          sourceIdentityProviderId: String,
          targetIdentityProviderId: String,
      ): Unit =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Users.UpdateIdp(
              id = id,
              sourceIdentityProviderId = sourceIdentityProviderId,
              targetIdentityProviderId = targetIdentityProviderId,
            )
          )
        }

      private def verifyOnlyModifiableFieldsWhereModified(
          srcUser: User,
          modifiedUser: User,
      ): Unit = {
        val withAllowedUpdatesReverted = modifiedUser.copy(
          primaryParty = srcUser.primaryParty,
          isDeactivated = srcUser.isDeactivated,
          annotations = srcUser.annotations,
        )
        if (withAllowedUpdatesReverted != srcUser) {
          throw ModifyingNonModifiableUserPropertiesError()
        }
      }

      private def doGet(id: String, identityProviderId: String): LedgerApiUser =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Users.Get(
              id = id,
              identityProviderId = identityProviderId,
            )
          )
        }

      @Help.Summary("Manage Ledger Api User Rights")
      @Help.Group("Ledger Api User Rights")
      object rights extends Helpful {

        @Help.Summary("Grant new rights to a user")
        @Help.Description("""Users are used to dynamically managing the rights given to Daml applications.
          |This function is used to grant new rights to an existing user.
          id: the id used to identify the given user
          actAs: the set of parties this user is allowed to act as
          readAs: the set of parties this user is allowed to read as
          participantAdmin: flag (default false) indicating if the user is allowed to use the admin commands of the Ledger Api
          identityProviderAdmin: flag (default false) indicating if the user is allowed to manage users and parties assigned to the same identity provider
          identityProviderId: identity provider id
          readAsAnyParty: flag (default false) indicating if the user is allowed to read as any party
          executeAs: the set of parties for whom this user is allowed to operate interactive submissions
          executeAsAnyParty: flag (default false) indicating if the user is allowed to operate interactive submissions as any party
          """)
        def grant(
            id: String,
            actAs: Set[PartyId] = Set(),
            readAs: Set[PartyId] = Set(),
            participantAdmin: Boolean = false,
            identityProviderAdmin: Boolean = false,
            identityProviderId: String = "",
            readAsAnyParty: Boolean = false,
            executeAs: Set[PartyId] = Set(),
            executeAsAnyParty: Boolean = false,
        ): UserRights =
          consoleEnvironment.run {
            ledgerApiCommand(
              LedgerApiCommands.Users.Rights.Grant(
                id = id,
                actAs = actAs.map(_.toLf),
                readAs = readAs.map(_.toLf),
                executeAs = executeAs.map(_.toLf),
                participantAdmin = participantAdmin,
                identityProviderAdmin = identityProviderAdmin,
                identityProviderId = identityProviderId,
                readAsAnyParty = readAsAnyParty,
                executeAsAnyParty = executeAsAnyParty,
              )
            ).flatMap(_ =>
              ledgerApiCommand(
                LedgerApiCommands.Users.Rights.List(
                  id = id,
                  identityProviderId = identityProviderId,
                )
              )
            )
          }

        @Help.Summary("Revoke user rights")
        @Help.Description("""Use to revoke specific rights from a user.
          id: the id used to identify the given user
          actAs: the set of parties this user should not be allowed to act as
          readAs: the set of parties this user should not be allowed to read as
          participantAdmin: if set to true, the participant admin rights will be removed
          identityProviderAdmin: if set to true, the identity provider admin rights will be removed
          identityProviderId: identity provider id
          readAsAnyParty: flag (default false) indicating if the user is allowed to read as any party
          executeAs: the set of parties for whom this user is allowed to operate interactive submissions
          executeAsAnyParty: flag (default false) indicating if the user is allowed to operate interactive submissions as any party
          """)
        def revoke(
            id: String,
            actAs: Set[PartyId] = Set(),
            readAs: Set[PartyId] = Set(),
            participantAdmin: Boolean = false,
            identityProviderAdmin: Boolean = false,
            identityProviderId: String = "",
            readAsAnyParty: Boolean = false,
            executeAs: Set[PartyId] = Set(),
            executeAsAnyParty: Boolean = false,
        ): UserRights =
          consoleEnvironment.run {
            ledgerApiCommand(
              LedgerApiCommands.Users.Rights.Revoke(
                id = id,
                actAs = actAs.map(_.toLf),
                readAs = readAs.map(_.toLf),
                executeAs = executeAs.map(_.toLf),
                participantAdmin = participantAdmin,
                identityProviderAdmin = identityProviderAdmin,
                identityProviderId = identityProviderId,
                readAsAnyParty = readAsAnyParty,
                executeAsAnyParty = executeAsAnyParty,
              )
            ).flatMap(_ =>
              ledgerApiCommand(
                LedgerApiCommands.Users.Rights.List(
                  id = id,
                  identityProviderId = identityProviderId,
                )
              )
            )
          }

        @Help.Summary("List rights of a user")
        @Help.Description("""Lists the rights of a user, or the rights of the current user.
            id: user id
            identityProviderId: identity provider id""")
        def list(id: String, identityProviderId: String = ""): UserRights =
          consoleEnvironment.run {
            ledgerApiCommand(
              LedgerApiCommands.Users.Rights.List(
                id = id,
                identityProviderId = identityProviderId,
              )
            )
          }
      }
    }

    @Help.Summary("Interact with the time service")
    @Help.Group("Time")
    object time {
      @Help.Summary("Get the participants time")
      @Help.Description("""Returns the current timestamp of the participant which is either the
                         system clock or the static time""")
      def get(): CantonTimestamp =
        consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.Time.Get)
        }

      @Help.Summary("Set the participants time", FeatureFlag.Testing)
      @Help.Description(
        """Sets the participants time if the participant is running in static time mode"""
      )
      def set(currentTime: CantonTimestamp, nextTime: CantonTimestamp): Unit =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.Time.Set(currentTime, nextTime))
        })
    }

    @Help.Summary("Query event details")
    @Help.Group("EventQuery")
    object event_query extends Helpful {

      @Help.Summary("Get events by contract Id")
      @Help.Description("""Return events associated with the given contract Id""")
      def by_contract_id(
          contractId: String,
          requestingParties: Seq[Party],
      ): GetEventsByContractIdResponse =
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.QueryService
              .GetEventsByContractId(contractId, requestingParties.map(_.toLf))
          )
        }
    }

    @Help.Summary("Group of commands that utilize java bindings")
    @Help.Group("Ledger Api (Java bindings)")
    object javaapi extends Helpful {

      @Help.Summary("Interactive submission")
      @Help.Group("Interactive Submission")
      object interactive_submission extends Helpful {

        @Help.Summary(
          "Prepare a transaction for interactive submission"
        )
        @Help.Description(
          "Prepare a transaction for interactive submission"
        )
        def prepare(
            actAs: Seq[PartyId],
            commands: Seq[javab.data.Command],
            synchronizerId: Option[SynchronizerId] = None,
            commandId: String = UUID.randomUUID().toString,
            minLedgerTimeAbs: Option[Instant] = None,
            readAs: Seq[Party] = Seq.empty,
            disclosedContracts: Seq[javab.data.DisclosedContract] = Seq.empty,
            userId: String = userId,
            userPackageSelectionPreference: Seq[LfPackageId] = Seq.empty,
            verboseHashing: Boolean = false,
            prefetchContractKeys: Seq[javab.data.PrefetchContractKey] = Seq.empty,
        ): PrepareResponseProto =
          consoleEnvironment.run {
            ledgerApiCommand(
              LedgerApiCommands.InteractiveSubmissionService.PrepareCommand(
                actAs.map(_.toLf),
                readAs.map(_.toLf),
                commands.map(c => Command.fromJavaProto(c.toProtoCommand)),
                commandId,
                minLedgerTimeAbs,
                disclosedContracts.map(c => DisclosedContract.fromJavaProto(c.toProto)),
                synchronizerId,
                userId,
                userPackageSelectionPreference,
                verboseHashing,
                prefetchContractKeys.map(k => PrefetchContractKey.fromJavaProto(k.toProto)),
              )
            )
          }
      }

      @Help.Summary("Submit commands (Java bindings)")
      @Help.Group("Command Submission (Java bindings)")
      object commands extends Helpful {

        @Help.Summary(
          "Submit java codegen command and wait for the resulting transaction, returning the transaction or failing otherwise"
        )
        @Help.Description(
          """Submits a command on behalf of the `actAs` parties, waits for the resulting transaction to commit, and returns the "flattened" transaction.
            | If the timeout is set, it also waits for the transaction to appear at all other configured
            | participants who were involved in the transaction. The call blocks until the transaction commits or fails;
            | the timeout only specifies how long to wait at the other participants.
            | Fails if the transaction doesn't commit, or if it doesn't become visible to the involved participants in
            | the allotted time.
            | Note that if the optTimeout is set and the involved parties are concurrently enabled/disabled or their
            | participants are connected/disconnected, the command may currently result in spurious timeouts or may
            | return before the transaction appears at all the involved participants."""
        )
        def submit(
            actAs: Seq[Party],
            commands: Seq[javab.data.Command],
            synchronizerId: Option[SynchronizerId] = None,
            workflowId: String = "",
            commandId: String = "",
            optTimeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
            deduplicationPeriod: Option[DeduplicationPeriod] = None,
            submissionId: String = "",
            minLedgerTimeAbs: Option[Instant] = None,
            readAs: Seq[Party] = Seq.empty,
            disclosedContracts: Seq[javab.data.DisclosedContract] = Seq.empty,
            userId: String = userId,
            userPackageSelectionPreference: Seq[LfPackageId] = Seq.empty,
            transactionShape: TransactionShape = TRANSACTION_SHAPE_ACS_DELTA,
            includeCreatedEventBlob: Boolean = false,
        ): Transaction = {
          val externalParties = actAs.collect { case externalParty: ExternalParty => externalParty }

          // TODO(#27461) Support multiple submitting parties
          if (externalParties.sizeIs > 1)
            consoleEnvironment.raiseError(
              s"submit supports at most one external party, found: ${externalParties.map(_.partyId)}"
            )

          externalParties.headOption match {
            case Some(externalParty) =>
              external.submit(
                externalParty,
                commands,
                synchronizerId,
                commandId,
                optTimeout,
                deduplicationPeriod,
                submissionId,
                minLedgerTimeAbs,
                readAs,
                disclosedContracts,
                userId,
                userPackageSelectionPreference,
                includeCreatedEventBlob = includeCreatedEventBlob,
              )

            case _ =>
              val tx = consoleEnvironment.run {
                ledgerApiCommand(
                  LedgerApiCommands.CommandService.SubmitAndWaitTransaction(
                    actAs.map(_.toLf),
                    readAs.map(_.toLf),
                    commands.map(c => Command.fromJavaProto(c.toProtoCommand)),
                    workflowId,
                    commandId,
                    deduplicationPeriod,
                    submissionId,
                    minLedgerTimeAbs,
                    disclosedContracts.map(c => DisclosedContract.fromJavaProto(c.toProto)),
                    synchronizerId,
                    userId,
                    userPackageSelectionPreference,
                    transactionShape,
                    includeCreatedEventBlob = includeCreatedEventBlob,
                  )
                )
              }

              javab.data.Transaction.fromProto(
                ApiTransaction.toJavaProto(
                  optionallyAwait(tx, tx.updateId, tx.synchronizerId, optTimeout)
                )
              )
          }
        }

        @Help.Summary("Submit java codegen command asynchronously")
        @Help.Description(
          """Provides access to the command submission service of the Ledger API.
            |See https://docs.daml.com/app-dev/services.html for documentation of the parameters."""
        )
        def submit_async(
            actAs: Seq[PartyId],
            commands: Seq[javab.data.Command],
            synchronizerId: Option[SynchronizerId] = None,
            workflowId: String = "",
            commandId: String = "",
            deduplicationPeriod: Option[DeduplicationPeriod] = None,
            submissionId: String = "",
            minLedgerTimeAbs: Option[Instant] = None,
            readAs: Seq[Party] = Seq.empty,
            disclosedContracts: Seq[javab.data.DisclosedContract] = Seq.empty,
            userId: String = userId,
        ): Unit =
          ledger_api.commands.submit_async(
            actAs,
            commands.map(c => Command.fromJavaProto(c.toProtoCommand)),
            synchronizerId,
            workflowId,
            commandId,
            deduplicationPeriod,
            submissionId,
            minLedgerTimeAbs,
            readAs,
            disclosedContracts.map(c => DisclosedContract.fromJavaProto(c.toProto)),
            userId,
          )

        @Help.Summary(
          "Submit assign command and wait for the resulting java codegen reassignment, returning the reassignment or failing otherwise"
        )
        @Help.Description(
          """Submits an unassignment command on behalf of `submitter` party, waits for the resulting unassignment to commit, and returns the reassignment.
            | If timeout is set, it also waits for the reassignment(s) to appear at all other
            | participants who were involved in the unassignment. The call blocks until the unassignment commits or fails.
            | Fails if the unassignment doesn't commit, or if it doesn't become visible to the involved participants in time.
            | Timeout specifies the time how long to wait until the reassignment appears in the update stream for the submitting and all the involved participants."""
        )
        def submit_unassign(
            submitter: PartyId,
            contractIds: Seq[LfContractId],
            source: SynchronizerId,
            target: SynchronizerId,
            workflowId: String = "",
            userId: String = userId,
            submissionId: String = UUID.randomUUID().toString,
            timeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
        ): Reassignment =
          ledger_api.commands
            .submit_unassign(
              submitter,
              contractIds,
              source,
              target,
              workflowId,
              userId,
              submissionId,
              timeout,
            )
            .unassignedWrapper
            .reassignment
            .pipe(ReassignmentProto.toJavaProto)
            .pipe(Reassignment.fromProto)

        @Help.Summary(
          "Submit assign command and wait for the resulting java codegen reassignment, returning the reassignment or failing otherwise"
        )
        @Help.Description(
          """Submits a assignment command on behalf of `submitter` party, waits for the resulting assignment to commit, and returns the reassignment.
            | If timeout is set, it also waits for the reassignment(s) to appear at all other
            | participants who were involved in the assignment. The call blocks until the assignment commits or fails.
            | Fails if the assignment doesn't commit, or if it doesn't become visible to the involved participants in time.
            | Timeout specifies the time how long to wait until the reassignment appears in the update stream for the submitting and all the involved participants.
            | The reassignmentId should be the one returned by the corresponding submit_unassign command."""
        )
        def submit_assign(
            submitter: PartyId,
            reassignmentId: String,
            source: SynchronizerId,
            target: SynchronizerId,
            workflowId: String = "",
            userId: String = userId,
            submissionId: String = UUID.randomUUID().toString,
            timeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
            includeCreatedEventBlob: Boolean = false,
        ): Reassignment =
          ledger_api.commands
            .submit_assign_with_format(
              submitter,
              reassignmentId,
              source,
              target,
              workflowId,
              userId,
              submissionId,
              eventFormat = eventFormatAllParties(includeCreatedEventBlob),
              timeout,
            )
            .reassignment
            .pipe(ReassignmentProto.toJavaProto)
            .pipe(Reassignment.fromProto)

        @Help.Summary("Submit commands on behalf of external parties")
        @Help.Group("Command Submission")
        private[canton] object external {
          def submit(
              actAs: ExternalParty, // TODO(#27461) Support multiple submitting parties
              commands: Seq[javab.data.Command],
              synchronizerId: Option[SynchronizerId] = None,
              commandId: String = "",
              optTimeout: Option[config.NonNegativeDuration] = Some(timeouts.ledgerCommand),
              deduplicationPeriod: Option[DeduplicationPeriod] = None,
              submissionId: String = "",
              minLedgerTimeAbs: Option[Instant] = None,
              readAs: Seq[Party] = Seq.empty,
              disclosedContracts: Seq[javab.data.DisclosedContract] = Seq.empty,
              userId: String = userId,
              userPackageSelectionPreference: Seq[LfPackageId] = Seq.empty,
              includeCreatedEventBlob: Boolean = false,
          ): Transaction = {
            val protoCommands = commands.map(_.toProtoCommand).map(Command.fromJavaProto)
            val protoDisclosedContracts =
              disclosedContracts.map(c => DisclosedContract.fromJavaProto(c.toProto))

            val tx = ledger_api.commands.external.submit(
              actAs = actAs,
              commands = protoCommands,
              synchronizerId = synchronizerId,
              commandId = commandId,
              optTimeout = optTimeout,
              deduplicationPeriod = deduplicationPeriod,
              submissionId = submissionId,
              minLedgerTimeAbs = minLedgerTimeAbs,
              readAs = readAs,
              disclosedContracts = protoDisclosedContracts,
              userId = userId,
              userPackageSelectionPreference = userPackageSelectionPreference,
              includeCreatedEventBlob = includeCreatedEventBlob,
            )

            javab.data.Transaction.fromProto(ApiTransaction.toJavaProto(tx))
          }
        }
      }

      @Help.Summary("Read from update stream (Java bindings)")
      @Help.Group("Updates (Java bindings)")
      object updates extends Helpful {

        @Help.Summary(
          "Get updates in the format expected by the Java bindings"
        )
        @Help.Description(
          """This function connects to the update stream for the given parties and collects updates
            |until either `completeAfter` updates have been received or `timeout` has elapsed.
            |The returned updates can be filtered to be between the given offsets (default: no filtering).
            |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
            |this command fails with a `NOT_FOUND` error.
            |If the beginOffset is zero then the participant begin is taken as beginning offset.
            |If the endOffset is None then a continuous stream is returned."""
        )
        def updates(
            updateFormat: UpdateFormat,
            completeAfter: PositiveInt,
            beginOffsetExclusive: Long = 0L,
            endOffsetInclusive: Option[Long] = None,
            timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
            resultFilter: UpdateWrapper => Boolean = _ => true,
            synchronizerFilter: Option[SynchronizerId] = None,
        ): Seq[GetUpdatesResponse] =
          ledger_api.updates
            .updates(
              updateFormat,
              completeAfter,
              beginOffsetExclusive,
              endOffsetInclusive,
              timeout,
              resultFilter,
              synchronizerFilter,
            )
            .map {
              case tx: TransactionWrapper =>
                tx.transaction
                  .pipe(ApiTransaction.toJavaProto)
                  .pipe(javab.data.Transaction.fromProto)
                  .pipe(new GetUpdatesResponse(_))

              case reassignment: ReassignmentWrapper =>
                reassignment.reassignment
                  .pipe(ReassignmentProto.toJavaProto)
                  .pipe(Reassignment.fromProto)
                  .pipe(new GetUpdatesResponse(_))

              case tt: TopologyTransactionWrapper =>
                tt.topologyTransaction
                  .pipe(TopoplogyTransactionProto.toJavaProto)
                  .pipe(TopologyTransaction.fromProto)
                  .pipe(new GetUpdatesResponse(_))
            }

        @Help.Summary(
          "Get transactions in the format expected by the Java bindings"
        )
        @Help.Description(
          """This function connects to the update stream for the given parties and collects updates
            |until either `completeAfter` transactions have been received or `timeout` has elapsed.
            |The returned updates can be filtered to be between the given offsets (default: no filtering).
            |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
            |this command fails with a `NOT_FOUND` error. If you need to specify filtering conditions for template IDs and
            |including create event blobs for explicit disclosure, consider using `tx_with_tx_format`.
            |If the beginOffset is zero then the participant begin is taken as beginning offset.
            |If the endOffset is None then a continuous stream is returned."""
        )
        def transactions(
            partyIds: Set[Party],
            completeAfter: PositiveInt,
            beginOffsetExclusive: Long = 0L,
            endOffsetInclusive: Option[Long] = None,
            verbose: Boolean = true,
            timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
            resultFilter: UpdateWrapper => Boolean = _ => true,
            synchronizerFilter: Option[SynchronizerId] = None,
            transactionShape: TransactionShape = TRANSACTION_SHAPE_ACS_DELTA,
        ): Seq[GetUpdatesResponse] =
          ledger_api.updates
            .transactions(
              partyIds,
              completeAfter,
              beginOffsetExclusive,
              endOffsetInclusive,
              verbose,
              timeout,
              resultFilter,
              synchronizerFilter,
              transactionShape,
            )
            .map {
              _.transaction
                .pipe(ApiTransaction.toJavaProto)
                .pipe(javab.data.Transaction.fromProto)
                .pipe(new GetUpdatesResponse(_))
            }

        @Help.Summary(
          "Get transactions in the format expected by the Java bindings"
        )
        @Help.Description(
          """This function connects to the update stream for the given transaction format and collects updates
            |until either `completeAfter` transactions have been received or `timeout` has elapsed.
            |The returned transactions can be filtered to be between the given offsets (default: no filtering).
            |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
            |this command fails with a `NOT_FOUND` error. If you only need to filter by a set of parties, consider using
            |`flat` or `trees` instead.
            |If the beginOffset is zero then the participant begin is taken as beginning offset.
            |If the endOffset is None then a continuous stream is returned."""
        )
        def transactions_with_tx_format(
            transactionFormat: TransactionFormat,
            completeAfter: PositiveInt,
            beginOffsetExclusive: Long = 0L,
            endOffsetInclusive: Option[Long] = None,
            timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
            resultFilter: UpdateWrapper => Boolean = _ => true,
        ): Seq[GetUpdatesResponse] =
          ledger_api.updates
            .transactions_with_tx_format(
              TransactionFormatProto.fromJavaProto(transactionFormat.toProto),
              completeAfter,
              beginOffsetExclusive,
              endOffsetInclusive,
              timeout,
              resultFilter,
            )
            .map {
              case tx: TransactionWrapper =>
                tx.transaction
                  .pipe(ApiTransaction.toJavaProto)
                  .pipe(javab.data.Transaction.fromProto)
                  .pipe(new GetUpdatesResponse(_))

              case reassignment: ReassignmentWrapper =>
                reassignment.reassignment
                  .pipe(ReassignmentProto.toJavaProto)
                  .pipe(Reassignment.fromProto)
                  .pipe(new GetUpdatesResponse(_))

              case tt: TopologyTransactionWrapper =>
                tt.topologyTransaction
                  .pipe(TopoplogyTransactionProto.toJavaProto)
                  .pipe(TopologyTransaction.fromProto)
                  .pipe(new GetUpdatesResponse(_))
            }
      }

      @Help.Summary("Collection of Ledger API state endpoints (Java bindings)")
      @Help.Group("State (Java bindings)")
      object state extends Helpful {

        @Help.Summary("Read active contracts (Java bindings)")
        @Help.Group("Active Contracts (Java bindings)")
        object acs extends Helpful {

          @Help.Summary(
            "Wait until a contract becomes available and return the Java codegen contract"
          )
          @Help.Description(
            """This function can be used for contracts with a code-generated Java model.
              |You can refine your search using the `filter` function argument.
              |You can restrict search to a synchronizer by specifying the optional synchronizer id.
              |The command will wait until the contract appears or throw an exception once it times out."""
          )
          def await[
              TC <: javab.data.codegen.Contract[TCid, T],
              TCid <: javab.data.codegen.ContractId[T],
              T <: javab.data.Template,
          ](companion: javab.data.codegen.ContractCompanion[TC, TCid, T])(
              partyId: Party,
              predicate: TC => Boolean = (_: TC) => true,
              synchronizerFilter: Option[SynchronizerId] = None,
              timeout: config.NonNegativeDuration = timeouts.ledgerCommand,
          ): TC = {
            val result = new AtomicReference[Option[TC]](None)
            ConsoleMacros.utils.retry_until_true(timeout) {
              val tmp = filter(companion)(partyId, predicate, synchronizerFilter)
              result.set(tmp.headOption)
              tmp.nonEmpty
            }
            consoleEnvironment.runE {
              result
                .get()
                .toRight(
                  s"Failed to find contract of type ${companion.TEMPLATE_ID} after $timeout"
                )
            }
          }

          @Help.Summary(
            "Filter the ACS for contracts of a particular Java code-generated template"
          )
          @Help.Description(
            """To use this function, ensure a code-generated Java model for the target template exists.
              |You can refine your search using the `predicate` function argument.
              |You can restrict search to a synchronizer by specifying the optional synchronizer id."""
          )
          def filter[
              TC <: javab.data.codegen.Contract[TCid, T],
              TCid <: javab.data.codegen.ContractId[T],
              T <: javab.data.Template,
          ](templateCompanion: javab.data.codegen.ContractCompanion[TC, TCid, T])(
              partyId: Party,
              predicate: TC => Boolean = (_: TC) => true,
              synchronizerFilter: Option[SynchronizerId] = None,
          ): Seq[TC] = {
            val templateId = TemplateId.fromJavaIdentifier(templateCompanion.TEMPLATE_ID)

            def synchronizerPredicate(entry: WrappedContractEntry) =
              synchronizerFilter match {
                case Some(_synchronizerId) => entry.synchronizerId == synchronizerFilter
                case None => true
              }

            ledger_api.state.acs
              .of_party(partyId, filterTemplates = Seq(templateId))
              .collect { case entry if synchronizerPredicate(entry) => entry.event }
              .flatMap { ev =>
                JavaDecodeUtil
                  .decodeCreated(templateCompanion)(
                    javab.data.CreatedEvent.fromProto(CreatedEvent.toJavaProto(ev))
                  )
                  .toList
              }
              .filter(predicate)
          }
        }
      }

      @Help.Summary("Query event details")
      @Help.Group("EventQuery")
      object event_query extends Helpful {

        @Help.Summary("Get events in java codegen by contract Id")
        @Help.Description("""Return events associated with the given contract Id""")
        def by_contract_id(
            contractId: String,
            requestingParties: Seq[Party],
        ): com.daml.ledger.api.v2.EventQueryServiceOuterClass.GetEventsByContractIdResponse =
          ledger_api.event_query
            .by_contract_id(contractId, requestingParties)
            .pipe(GetEventsByContractIdResponse.toJavaProto)
      }
    }
  }

  /** @return
    *   The modified map where deletion from the original are represented as keys with empty values
    */
  private def makeAnnotationsUpdate(
      original: Map[String, String],
      modified: Map[String, String],
  ): Map[String, String] = {
    val deletions = original.removedAll(modified.keys).view.mapValues(_ => "").toMap
    modified.concat(deletions)
  }
}

trait LedgerApiAdministration extends BaseLedgerApiAdministration {
  this: LedgerApiCommandRunner & AdminCommandRunner & NamedLogging & FeatureFlagFilter =>

  implicit protected val consoleEnvironment: ConsoleEnvironment
  protected val name: String

  import com.digitalasset.canton.util.ShowUtil.*

  private def wildcardUpdateFormatForParties(parties: List[Party]): UpdateFormat = {
    val eventFormat = EventFormat(
      filtersByParty = parties.toSeq
        .map(party =>
          party.toLf -> Filters(
            Seq(
              CumulativeFilter(
                IdentifierFilter.WildcardFilter(
                  transaction_filter.WildcardFilter(includeCreatedEventBlob = false)
                )
              )
            )
          )
        )
        .toMap,
      filtersForAnyParty = None,
      verbose = false,
    )
    UpdateFormat(
      includeTransactions =
        Some(TransactionFormatProto(Some(eventFormat), TRANSACTION_SHAPE_LEDGER_EFFECTS)),
      includeReassignments = Some(eventFormat),
      includeTopologyEvents = None,
    )
  }

  private def awaitUpdate(
      updateId: String,
      at: Map[ParticipantReference, Party],
      timeout: config.NonNegativeDuration,
  ): Unit = {
    def scan() =
      at.map { case (participant, party) =>
        (
          participant,
          party,
          participant.ledger_api.updates
            .update_by_id(updateId, wildcardUpdateFormatForParties(List(party)))
            .isDefined,
        )
      }
    ConsoleMacros.utils.retry_until_true(timeout)(
      scan().forall { case (_, _, updateFound) => updateFound }, {
        val res = scan().map { case (participant, party, res) =>
          s"${party.toString}@${participant.toString}: ${if (res) "observed" else "not observed"}"
        }
        s"Failed to observe update on all nodes: ${res.mkString(", ")}"
      },
    )

  }

  private[canton] def involvedParticipants(
      updateId: String,
      txSynchronizerId: String,
  ): Map[ParticipantReference, Party] = {
    val txSynchronizer = SynchronizerId.tryFromString(txSynchronizerId)
    // TODO(#6317)
    // There's a race condition here, in the unlikely circumstance that the party->participant mapping on the synchronizer
    // changes during the command's execution. We'll have to live with it for the moment, as there's no convenient
    // way to get the record time of the transaction to pass to the parties.list call.
    val synchronizerPartiesAndParticipants =
      consoleEnvironment.participants.all.iterator
        .filter(x => x.health.is_running() && x.health.initialized() && x.name == name)
        .flatMap(_.parties.list(synchronizerIds = Set(txSynchronizer)))
        .toList
        .distinct

    val synchronizerParties = synchronizerPartiesAndParticipants.map(_.party)
    // Read the transaction under the authority of all parties on the synchronizer, in order to get the witness_parties
    // to be all the actual witnesses of the transaction. There's no other convenient way to get the full witnesses,
    // as the Exercise events don't contain the informees of the Exercise action.
    val update = ledger_api.updates
      .update_by_id(updateId, wildcardUpdateFormatForParties(synchronizerParties))
      .getOrElse(
        throw new IllegalStateException(
          s"Can't find update by ID: $updateId. Queried parties: $synchronizerParties"
        )
      )
    val witnesses = (update match {
      case TransactionWrapper(tx) =>
        tx.events.flatMap { ev =>
          ev.event.created.fold(Seq.empty[String])(ev => ev.witnessParties) ++
            ev.event.exercised.fold(Seq.empty[String])(ev => ev.witnessParties)
        }

      case AssignedWrapper(_, assignedEvents) =>
        assignedEvents
          .flatMap(_.createdEvent)
          .flatMap(_.witnessParties)

      case UnassignedWrapper(_, unassignedEvents) =>
        unassignedEvents
          .flatMap(_.witnessParties)

      case TopologyTransactionWrapper(_) =>
        throw new IllegalStateException(
          s"Invalid: update by ID: $updateId belongs to a topology transaction"
        )
    }).map(PartyId.tryFromProtoPrimitive).toSet

    // A participant identity equality check that doesn't blow up if the participant isn't running
    def identityIs(pRef: ParticipantReference, id: ParticipantId): Boolean = pRef match {
      case lRef: LocalParticipantReference =>
        lRef.is_running && lRef.health.initialized() && lRef.id == id
      case rRef: RemoteParticipantReference =>
        rRef.health.initialized() && rRef.id == id
      case _ => false
    }

    // Map each involved participant to some party that witnessed the transaction (it doesn't matter which one)
    synchronizerPartiesAndParticipants.toList.foldMapK { cand =>
      if (witnesses.contains(cand.party)) {
        val involvedConsoleParticipants = cand.participants.mapFilter { pd =>
          for {
            participantReference <-
              consoleEnvironment.participants.all
                .filter(x => x.health.is_running() && x.health.initialized())
                .find(identityIs(_, pd.participant))
            _ <- pd.synchronizers.find(_.synchronizerId == txSynchronizer)
          } yield participantReference
        }
        involvedConsoleParticipants
          .map(_ -> cand.party)
          .toMap
      } else Map.empty
    }
  }

  override private[canton] def optionallyAwait[T](
      update: T,
      updateId: String,
      txSynchronizerId: String,
      optTimeout: Option[config.NonNegativeDuration],
  ): T =
    optTimeout match {
      case None => update
      case Some(timeout) =>
        val involved = involvedParticipants(updateId, txSynchronizerId)
        logger.debug(show"Awaiting update ${updateId.unquoted} at ${involved.keys.mkShow()}")
        awaitUpdate(updateId, involved, timeout)
        update
    }
}
