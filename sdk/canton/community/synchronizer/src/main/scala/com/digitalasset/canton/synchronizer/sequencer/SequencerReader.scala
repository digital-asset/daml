// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.{EitherT, OptionT}
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  CantonConfigValidationError,
  CantonConfigValidator,
  CantonEdition,
  CustomCantonConfigValidation,
  EnterpriseCantonEdition,
  NonNegativeFiniteDuration,
  ProcessingTimeout,
}
import com.digitalasset.canton.crypto.SyncCryptoError.KeyNotAvailable
import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.SequencedEventValidator.TopologyTimestampVerificationError
import com.digitalasset.canton.sequencing.client.SequencerSubscriptionError.SequencedEventError
import com.digitalasset.canton.sequencing.client.{
  SequencedEventValidator,
  SequencerSubscriptionError,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.sequencing.{GroupAddressResolver, OrdinarySerializedEvent}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.synchronizer.sequencer.SequencerReader.ReadState
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.synchronizer.sequencer.store.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.PekkoUtil.{
  CombinedKillSwitch,
  KillSwitchFlagCloseable,
  WithKillSwitch,
  sinkIgnoreFUS,
}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerCounter, config}
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.{Done, NotUsed}

import java.sql.SQLTransientConnectionException
import scala.concurrent.ExecutionContext

/** We throw this if a
  * [[com.digitalasset.canton.synchronizer.sequencer.store.SaveCounterCheckpointError.CounterCheckpointInconsistent]]
  * error is returned when saving a new member counter checkpoint. This is exceptionally concerning
  * as may suggest that we are streaming events with inconsistent counters. Should only be caused by
  * a bug or the datastore being corrupted.
  */
class CounterCheckpointInconsistentException(message: String) extends RuntimeException(message)

/** Configuration for the database based sequence reader.
  * @param readBatchSize
  *   max number of events to fetch from the datastore in one page
  * @param checkpointInterval
  *   how frequently to checkpoint state
  * @param pollingInterval
  *   how frequently to poll for new events from the database. only used in the enterprise edition
  *   if high availability has been configured, otherwise will rely on local writes performed by
  *   this sequencer to indicate that new events are available.
  * @param payloadBatchSize
  *   max number of payloads to fetch from the datastore in one page
  * @param payloadBatchWindow
  *   max time window to wait for more payloads before fetching the current batch from the datastore
  * @param payloadFetchParallelism
  *   how many batches of payloads will be fetched in parallel
  * @param eventGenerationParallelism
  *   how many events will be generated from the fetched payloads in parallel
  */
final case class SequencerReaderConfig(
    readBatchSize: Int = SequencerReaderConfig.defaultReadBatchSize,
    checkpointInterval: config.NonNegativeFiniteDuration =
      SequencerReaderConfig.defaultCheckpointInterval,
    pollingInterval: Option[NonNegativeFiniteDuration] = None,
    payloadBatchSize: Int = SequencerReaderConfig.defaultPayloadBatchSize,
    payloadBatchWindow: config.NonNegativeFiniteDuration =
      SequencerReaderConfig.defaultPayloadBatchWindow,
    payloadFetchParallelism: Int = SequencerReaderConfig.defaultPayloadFetchParallelism,
    eventGenerationParallelism: Int = SequencerReaderConfig.defaultEventGenerationParallelism,
) extends CustomCantonConfigValidation {
  override protected def doValidate(edition: CantonEdition): Seq[CantonConfigValidationError] =
    Option
      .when(pollingInterval.nonEmpty && edition != EnterpriseCantonEdition)(
        CantonConfigValidationError(
          s"Configuration polling-interval is supported only in $EnterpriseCantonEdition"
        )
      )
      .toList
}

object SequencerReaderConfig {
  implicit val sequencerReaderConfigCantonConfigValidator
      : CantonConfigValidator[SequencerReaderConfig] =
    CantonConfigValidatorDerivation[SequencerReaderConfig]

  val defaultReadBatchSize: Int = 100
  val defaultCheckpointInterval: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofSeconds(5)
  val defaultPayloadBatchSize: Int = 10
  val defaultPayloadBatchWindow: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofMillis(5)
  val defaultPayloadFetchParallelism: Int = 2
  val defaultEventGenerationParallelism: Int = 4

  /** The default polling interval if [[SequencerReaderConfig.pollingInterval]] is unset despite
    * high availability being configured.
    */
  val defaultPollingInterval = NonNegativeFiniteDuration.ofMillis(50)
}

class SequencerReader(
    config: SequencerReaderConfig,
    synchronizerId: SynchronizerId,
    store: SequencerStore,
    syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
    eventSignaller: EventSignaller,
    topologyClientMember: Member,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    blockSequencerMode: Boolean,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  def read(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.EventSource] =
    performUnlessClosingEitherUSF(functionFullName)(for {
      registeredTopologyClientMember <- EitherT
        .fromOptionF(
          store.lookupMember(topologyClientMember),
          CreateSubscriptionError.UnknownMember(topologyClientMember),
        )
        .leftWiden[CreateSubscriptionError]
      registeredMember <- EitherT
        .fromOptionF(
          store.lookupMember(member),
          CreateSubscriptionError.UnknownMember(member),
        )
        .leftWiden[CreateSubscriptionError]
      // check they haven't been disabled
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        registeredMember.enabled,
        CreateSubscriptionError.MemberDisabled(member): CreateSubscriptionError,
      )
      // We use the sequencing time of the topology transaction that registered the member on the synchronizer
      // as the latestTopologyClientRecipientTimestamp
      memberOnboardingTxSequencingTime <- EitherT.right(
        syncCryptoApi.headSnapshot.ipsSnapshot
          .memberFirstKnownAt(member)
          .map {
            case Some((sequencedTime, _)) => sequencedTime.value
            case None =>
              ErrorUtil.invalidState(
                s"Member $member unexpectedly not known to the topology client"
              )
          }
      )
      initialReadState <- EitherT.right(
        startFromClosestCounterCheckpoint(
          ReadState.initial(
            member,
            registeredMember,
            latestTopologyClientRecipientTimestamp = memberOnboardingTxSequencingTime,
          ),
          offset,
        )
      )
      // validate we are in the bounds of the data that this sequencer can serve
      lowerBoundO <- EitherT.right(store.fetchLowerBound())
      _ <- EitherT
        .cond[FutureUnlessShutdown](
          lowerBoundO.forall(_ <= initialReadState.nextReadTimestamp),
          (), {
            val lowerBoundText = lowerBoundO.map(_.toString).getOrElse("epoch")
            val errorMessage =
              show"Subscription for $member@$offset would require reading data from ${initialReadState.nextReadTimestamp} but our lower bound is ${lowerBoundText.unquoted}."

            logger.error(errorMessage)
            CreateSubscriptionError.EventsUnavailable(offset, errorMessage)
          },
        )
        .leftWiden[CreateSubscriptionError]
    } yield {
      val loggerFactoryForMember = loggerFactory.append("subscriber", member.toString)
      val reader = new EventsReader(
        member,
        registeredMember,
        registeredTopologyClientMember.memberId,
        loggerFactoryForMember,
      )
      reader.from(offset, initialReadState)
    })

  private[SequencerReader] class EventsReader(
      member: Member,
      registeredMember: RegisteredMember,
      topologyClientMemberId: SequencerMemberId,
      override protected val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging {

    import SequencerReader.*

    private def unvalidatedEventsSourceFromCheckpoint(initialReadState: ReadState)(implicit
        traceContext: TraceContext
    ): Source[(SequencerCounter, Sequenced[IdOrPayload]), NotUsed] =
      eventSignaller
        .readSignalsForMember(member, registeredMember.memberId)
        .via(
          FetchLatestEventsFlow[(SequencerCounter, Sequenced[IdOrPayload]), ReadState](
            initialReadState,
            state => fetchUnvalidatedEventsBatchFromCheckpoint(state)(traceContext),
            (state, _) => !state.lastBatchWasFull,
          )
        )

    /** An Pekko flow that passes the [[UnsignedEventData]] untouched from input to output, but
      * asynchronously records every checkpoint interval. The materialized future completes when all
      * checkpoints have been recorded after the kill switch has been pulled.
      */
    private def recordCheckpointFlow(implicit
        traceContext: TraceContext
    ): Flow[UnsignedEventData, UnsignedEventData, (KillSwitch, FutureUnlessShutdown[Done])] = {
      val recordCheckpointSink
          : Sink[UnsignedEventData, (KillSwitch, FutureUnlessShutdown[Done])] = {
        // in order to make sure database operations do not keep being retried (in case of connectivity issues)
        // after we start closing the subscription, we create a flag closeable that gets closed when this
        // subscriptions kill switch is activated. This flag closeable is wrapped in a close context below
        // which is passed down to saveCounterCheckpoint.
        val killSwitchFlagCloseable =
          FlagCloseable(SequencerReader.this.logger, SequencerReader.this.timeouts)
        val closeContextKillSwitch = new KillSwitchFlagCloseable(killSwitchFlagCloseable)
        Flow[UnsignedEventData]
          .buffer(1, OverflowStrategy.dropTail) // we only really need one event and can drop others
          .throttle(1, config.checkpointInterval.underlying)
          // The kill switch must sit after the throttle because throttle will pass the completion downstream
          // only after the bucket with unprocessed events has been drained, which happens only every checkpoint interval
          .viaMat(KillSwitches.single)(Keep.right)
          .mapMaterializedValue(killSwitch =>
            new CombinedKillSwitch(killSwitch, closeContextKillSwitch)
          )
          .mapAsyncUS(parallelism = 1) { unsignedEventData =>
            val event = unsignedEventData.event
            logger.debug(s"Preparing counter checkpoint for $member at ${event.timestamp}")
            val checkpoint =
              CounterCheckpoint(event, unsignedEventData.latestTopologyClientTimestamp)
            performUnlessClosingUSF(functionFullName) {
              implicit val closeContext: CloseContext = CloseContext(killSwitchFlagCloseable)
              saveCounterCheckpoint(member, registeredMember.memberId, checkpoint)
            }.recover {
              case e: SQLTransientConnectionException if killSwitchFlagCloseable.isClosing =>
                // after the subscription is closed, any retries will stop and possibly return an error
                // if there are connection problems with the db at the time of subscription close.
                // so in order to cleanly shutdown, we should recover from this kind of error.
                logger.debug(
                  "Database connection problems while closing subscription. It can be safely ignored.",
                  e,
                )
                UnlessShutdown.unit
            }
          }
          .toMat(sinkIgnoreFUS)(Keep.both)
      }

      Flow[UnsignedEventData].wireTapMat(recordCheckpointSink)(Keep.right)
    }

    private def signValidatedEvent(
        unsignedEventData: UnsignedEventData
    ): EitherT[FutureUnlessShutdown, SequencedEventError, OrdinarySerializedEvent] = {
      val UnsignedEventData(
        event,
        topologySnapshotO,
        previousTopologyClientTimestamp,
        latestTopologyClientTimestamp,
        eventTraceContext,
      ) = unsignedEventData
      implicit val traceContext: TraceContext = eventTraceContext
      logger.trace(
        s"Latest topology client timestamp for $member at counter ${event.counter} / ${event.timestamp} is $previousTopologyClientTimestamp / $latestTopologyClientTimestamp"
      )

      val res = for {
        signingSnapshot <- OptionT
          .fromOption[FutureUnlessShutdown](topologySnapshotO)
          .getOrElseF {
            val warnIfApproximate = event.counter > SequencerCounter.Genesis
            SyncCryptoClient.getSnapshotForTimestamp(
              syncCryptoApi,
              event.timestamp,
              previousTopologyClientTimestamp,
              protocolVersion,
              warnIfApproximate = warnIfApproximate,
            )
          }
        _ = logger.debug(
          s"Signing event with counter ${event.counter} / timestamp ${event.timestamp} for $member"
        )
        signed <- performUnlessClosingUSF("sign-event")(
          signEvent(event, signingSnapshot).value
        )
      } yield signed
      EitherT(res)
    }

    def latestTopologyClientTimestampAfter(
        topologyClientTimestampBefore: Option[CantonTimestamp],
        event: Sequenced[?],
    ): Option[CantonTimestamp] = {
      val addressedToTopologyClient = event.event.members.contains(topologyClientMemberId)
      if (addressedToTopologyClient) Some(event.timestamp)
      else topologyClientTimestampBefore
    }

    private val emptyBatch = Batch.empty[ClosedEnvelope](protocolVersion)
    private type TopologyClientTimestampAfter = Option[CantonTimestamp]

    case class ValidatedSnapshotWithEvent[P](
        topologyClientTimestampBefore: Option[CantonTimestamp],
        snapshotOrError: Option[
          Either[(CantonTimestamp, TopologyTimestampVerificationError), SyncCryptoApi]
        ],
        counter: SequencerCounter,
        unvalidatedEvent: Sequenced[P],
    ) {
      def mapEventPayload[Q](f: P => Q): ValidatedSnapshotWithEvent[Q] =
        ValidatedSnapshotWithEvent[Q](
          topologyClientTimestampBefore,
          snapshotOrError,
          counter,
          unvalidatedEvent.map(f),
        )
    }

    def validateEvent(
        topologyClientTimestampBefore: Option[CantonTimestamp],
        sequenced: (SequencerCounter, Sequenced[IdOrPayload]),
    ): FutureUnlessShutdown[
      (TopologyClientTimestampAfter, ValidatedSnapshotWithEvent[IdOrPayload])
    ] = {
      val (counter, unvalidatedEvent) = sequenced

      def validateTopologyTimestamp(
          topologyTimestamp: CantonTimestamp,
          sequencingTimestamp: CantonTimestamp,
          eventTraceContext: TraceContext,
      ): FutureUnlessShutdown[
        (TopologyClientTimestampAfter, ValidatedSnapshotWithEvent[IdOrPayload])
      ] = {
        implicit val traceContext: TraceContext = eventTraceContext
        // The topology timestamp will end up as the timestamp of topology on the signed event.
        // So we validate it accordingly.
        SequencedEventValidator
          .validateTopologyTimestamp(
            syncCryptoApi,
            topologyTimestamp,
            sequencingTimestamp,
            topologyClientTimestampBefore,
            protocolVersion,
            // This warning should never be triggered.
            warnIfApproximate = true,
            _.sequencerTopologyTimestampTolerance,
          )
          .value
          .map { snapshotOrError =>
            val topologyClientTimestampAfter = snapshotOrError.fold(
              _ => topologyClientTimestampBefore,
              _ =>
                latestTopologyClientTimestampAfter(topologyClientTimestampBefore, unvalidatedEvent),
            )

            topologyClientTimestampAfter -> ValidatedSnapshotWithEvent(
              topologyClientTimestampBefore,
              Some(snapshotOrError.leftMap(topologyTimestamp -> _)),
              counter,
              unvalidatedEvent,
            )
          }
      }

      unvalidatedEvent.event.topologyTimestampO match {
        // Deliver and receipt events with a topologyTimestamp must be validated
        case Some(topologyTimestamp) =>
          validateTopologyTimestamp(
            topologyTimestamp,
            unvalidatedEvent.timestamp,
            unvalidatedEvent.event.traceContext,
          )
        // Errors; delivers and receipts with no topologyTimestamp specified bypass validation
        case None =>
          val after =
            latestTopologyClientTimestampAfter(topologyClientTimestampBefore, unvalidatedEvent)
          FutureUnlessShutdown.pure(
            after -> ValidatedSnapshotWithEvent(
              topologyClientTimestampBefore,
              None,
              counter,
              unvalidatedEvent,
            )
          )

      }
    }

    def generateEvent(
        snapshotWithEvent: ValidatedSnapshotWithEvent[Batch[ClosedEnvelope]]
    ): FutureUnlessShutdown[UnsignedEventData] = {
      implicit val traceContext = snapshotWithEvent.unvalidatedEvent.traceContext
      import snapshotWithEvent.{counter, topologyClientTimestampBefore, unvalidatedEvent}

      def validationSuccess(
          eventF: FutureUnlessShutdown[SequencedEvent[ClosedEnvelope]],
          signingSnapshot: Option[SyncCryptoApi],
      ): FutureUnlessShutdown[UnsignedEventData] = {
        val topologyClientTimestampAfter =
          latestTopologyClientTimestampAfter(topologyClientTimestampBefore, unvalidatedEvent)
        eventF
          .map { eventEnvelope =>
            UnsignedEventData(
              eventEnvelope,
              signingSnapshot,
              topologyClientTimestampBefore,
              topologyClientTimestampAfter,
              unvalidatedEvent.traceContext,
            )
          }
      }

      snapshotWithEvent.snapshotOrError match {
        case None =>
          val eventF =
            mkSequencedEvent(
              counter,
              unvalidatedEvent,
              None,
              topologyClientTimestampBefore,
            )(unvalidatedEvent.traceContext)
          validationSuccess(eventF, None)

        case Some(Right(topologySnapshot)) =>
          val eventF =
            mkSequencedEvent(
              counter,
              unvalidatedEvent,
              Some(topologySnapshot.ipsSnapshot),
              topologyClientTimestampBefore,
            )(unvalidatedEvent.traceContext)
          validationSuccess(eventF, Some(topologySnapshot))

        case Some(
              Left(
                (topologyTimestamp, SequencedEventValidator.TopologyTimestampAfterSequencingTime)
              )
            ) =>
          // The SequencerWriter makes sure that the signing timestamp is at most the sequencing timestamp
          ErrorUtil.internalError(
            new IllegalArgumentException(
              s"The topology timestamp $topologyTimestamp must be before or at the sequencing timestamp ${unvalidatedEvent.timestamp} for sequencer counter $counter of member $member"
            )
          )

        case Some(
              Left(
                (
                  topologyTimestamp,
                  SequencedEventValidator.TopologyTimestampTooOld(_) |
                  SequencedEventValidator.NoDynamicSynchronizerParameters(_),
                )
              )
            ) =>
          // We can't use the topology timestamp for the sequencing time.
          // Replace the event with an error that is only sent to the sender
          // To not introduce gaps in the sequencer counters,
          // we deliver an empty batch to the member if it is not the sender.
          // This way, we can avoid revalidating the skipped events after the checkpoint we resubscribe from.
          val event = if (registeredMember.memberId == unvalidatedEvent.event.sender) {
            val error =
              SequencerErrors.TopologyTimestampTooEarly(
                topologyTimestamp,
                unvalidatedEvent.timestamp,
              )
            DeliverError.create(
              counter,
              unvalidatedEvent.timestamp,
              synchronizerId,
              unvalidatedEvent.event.messageId,
              error,
              protocolVersion,
              trafficReceiptForNonSequencerSender(
                unvalidatedEvent.event.sender,
                unvalidatedEvent.event.trafficReceiptO,
              ),
            )
          } else {
            Deliver.create(
              counter,
              unvalidatedEvent.timestamp,
              synchronizerId,
              None,
              emptyBatch,
              None,
              protocolVersion,
              None,
            )
          }

          // This event cannot change the topology state of the client
          // and might not reach the topology client even
          // if it was originally addressed to it.
          // So keep the before timestamp
          FutureUnlessShutdown.pure(
            UnsignedEventData(
              event,
              None,
              topologyClientTimestampBefore,
              topologyClientTimestampBefore,
              unvalidatedEvent.traceContext,
            )
          )
      }
    }

    private def fetchPayloadsForEventsBatch()(implicit
        traceContext: TraceContext
    ): Flow[WithKillSwitch[ValidatedSnapshotWithEvent[IdOrPayload]], UnsignedEventData, NotUsed] =
      Flow[WithKillSwitch[ValidatedSnapshotWithEvent[IdOrPayload]]]
        .groupedWithin(config.payloadBatchSize, config.payloadBatchWindow.underlying)
        .mapAsyncAndDrainUS(config.payloadFetchParallelism) { snapshotsWithEvent =>
          // fetch payloads in bulk
          val idOrPayloads =
            snapshotsWithEvent.flatMap(_.value.unvalidatedEvent.event.payloadO.toList)
          store
            .readPayloads(idOrPayloads, member)
            .map { loadedPayloads =>
              snapshotsWithEvent.map(snapshotWithEvent =>
                snapshotWithEvent.map(_.mapEventPayload {
                  case id: PayloadId =>
                    loadedPayloads.getOrElse(
                      id,
                      ErrorUtil.invalidState(
                        s"Event ${snapshotWithEvent.value.unvalidatedEvent.event.messageId} specified payloadId $id but no corresponding payload was found."
                      ),
                    )
                  case payload: BytesPayload => payload.decodeBatchAndTrim(protocolVersion, member)
                  case batch: FilteredBatch => Batch.trimForMember(batch.batch, member)
                })
              )
            }
            .tapOnShutdown(snapshotsWithEvent.headOption.foreach(_.killSwitch.shutdown()))
        }
        // generate events must be called one-by-one on the events in the stream so that events are released as early as possible.
        // otherwise we might run into a deadlock, where one event is waiting (forever) for a previous event to be fully
        // processed by the message processor pipeline to advance the topology client. but this never happens if generating those
        // events happens in the same async unit.
        // i.e. don't do: snapshotsWithEvent.parTraverse(generate)
        .mapConcat(identity)
        .mapAsyncAndDrainUS(config.eventGenerationParallelism)(validatedSnapshotsWithEvents =>
          generateEvent(validatedSnapshotsWithEvents.value)
            .tapOnShutdown(validatedSnapshotsWithEvents.killSwitch.shutdown())
        )

    def from(startAt: SequencerCounter, initialReadState: ReadState)(implicit
        traceContext: TraceContext
    ): Sequencer.EventSource = {
      val unvalidatedEventsSrc = unvalidatedEventsSourceFromCheckpoint(initialReadState)
      val validatedEventSrc = unvalidatedEventsSrc.statefulMapAsyncUSAndDrain(
        initialReadState.latestTopologyClientRecipientTimestamp
      )(validateEvent)
      val eventsSource =
        validatedEventSrc
          // drop events we don't care about before fetching payloads
          .dropWhile(_.counter < startAt)
          .viaMat(KillSwitches.single)(Keep.both)
          .injectKillSwitch { case (_, killSwitch) => killSwitch }
          .via(fetchPayloadsForEventsBatch())

      eventsSource
        .viaMat(
          if (blockSequencerMode) {
            // We don't need to reader-side checkpoints for the unified mode
            // TODO(#20910): Remove this in favor of periodic checkpoints
            Flow[UnsignedEventData].viaMat(KillSwitches.single) { case (_, killSwitch) =>
              (killSwitch, FutureUnlessShutdown.pure(Done))
            }
          } else {
            recordCheckpointFlow
          }
        )(Keep.right)
        .viaMat(KillSwitches.single) { case ((checkpointKillSwitch, checkpointDone), killSwitch) =>
          (new CombinedKillSwitch(checkpointKillSwitch, killSwitch), checkpointDone)
        }
        .mapAsyncAndDrainUS(
          // We technically do not need to process everything sequentially here.
          // Neither do we have evidence that parallel processing helps, as a single sequencer reader
          // will typically serve many subscriptions in parallel.
          parallelism = 1
        )(
          signValidatedEvent(_).value
        )
    }

    /** Attempt to save the counter checkpoint and fail horribly if we find this is an inconsistent
      * checkpoint update.
      */
    private def saveCounterCheckpoint(
        member: Member,
        memberId: SequencerMemberId,
        checkpoint: CounterCheckpoint,
    )(implicit
        traceContext: TraceContext,
        closeContext: CloseContext,
    ): FutureUnlessShutdown[Unit] = {
      logger.debug(s"Saving counter checkpoint for [$member] with value [$checkpoint]")

      store.saveCounterCheckpoint(memberId, checkpoint).valueOr {
        case SaveCounterCheckpointError.CounterCheckpointInconsistent(
              existingTimestamp,
              existingLatestTopologyClientTimestamp,
            ) =>
          val message =
            s"""|There is an existing checkpoint for member [$member] ($memberId) at counter ${checkpoint.counter} with timestamp $existingTimestamp and latest topology client timestamp $existingLatestTopologyClientTimestamp.
                |We attempted to write ${checkpoint.timestamp} and ${checkpoint.latestTopologyClientTimestamp}.""".stripMargin
          ErrorUtil.internalError(new CounterCheckpointInconsistentException(message))
      }
    }

    private def fetchUnvalidatedEventsBatchFromCheckpoint(
        readState: ReadState
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[(ReadState, Seq[(SequencerCounter, Sequenced[IdOrPayload])])] =
      for {
        readEvents <- store.readEvents(
          readState.memberId,
          readState.member,
          readState.nextReadTimestamp.some,
          config.readBatchSize,
        )
      } yield {
        // we may be rebuilding counters from a checkpoint before what was actually requested
        // in which case don't return events that we don't need to serve
        val nextSequencerCounter = readState.nextCounterAccumulator
        val eventsWithCounter =
          readEvents.events.zipWithIndex.map { case (event, n) =>
            (nextSequencerCounter + n, event)
          }
        val newReadState = readState.update(readEvents, config.readBatchSize)
        if (newReadState.nextReadTimestamp < readState.nextReadTimestamp) {
          ErrorUtil.invalidState(
            s"Read state is going backwards in time: ${newReadState.changeString(readState)}"
          )
        }
        if (logger.underlying.isDebugEnabled) {
          newReadState.changeString(readState).foreach(logger.debug(_))
        }
        (newReadState, eventsWithCounter)
      }

    private def signEvent(
        event: SequencedEvent[ClosedEnvelope],
        topologySnapshot: SyncCryptoApi,
    )(implicit traceContext: TraceContext): EitherT[
      FutureUnlessShutdown,
      SequencerSubscriptionError.TombstoneEncountered.Error,
      OrdinarySerializedEvent,
    ] =
      for {
        signedEvent <- SignedContent
          .create(
            topologySnapshot.pureCrypto,
            topologySnapshot,
            event,
            None,
            HashPurpose.SequencedEventSignature,
            protocolVersion,
          )
          .leftMap {
            case err @ KeyNotAvailable(_owner, _keyPurpose, _timestamp, candidates)
                if candidates.isEmpty =>
              logger.debug(s"Generating tombstone due to: $err")
              val error =
                SequencerSubscriptionError.TombstoneEncountered.Error(
                  event.counter,
                  member,
                  topologySnapshot.ipsSnapshot.timestamp,
                )
              logger.warn(error.cause)
              error
            case err =>
              throw new IllegalStateException(s"Signing failed with an unexpected error: $err")
          }
      } yield OrdinarySequencedEvent(signedEvent)(traceContext)

    private def trafficReceiptForNonSequencerSender(
        senderMemberId: SequencerMemberId,
        trafficReceiptO: Option[TrafficReceipt],
    ) =
      if (member.code == SequencerId.Code || registeredMember.memberId != senderMemberId) None
      else trafficReceiptO

    /** Takes our stored event and turns it back into a real sequenced event.
      */
    private def mkSequencedEvent(
        counter: SequencerCounter,
        event: Sequenced[Batch[ClosedEnvelope]],
        topologySnapshotO: Option[
          TopologySnapshot
        ], // only specified for DeliverStoreEvent, as errors are only sent to the sender
        topologyClientTimestampBeforeO: Option[
          CantonTimestamp
        ], // None for until the first topology event, otherwise contains the latest topology event timestamp
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[SequencedEvent[ClosedEnvelope]] = {
      val timestamp = event.timestamp
      event.event match {
        case DeliverStoreEvent(
              sender,
              messageId,
              _recipients,
              batch,
              topologyTimestampO,
              _traceContext,
              trafficReceiptO,
            ) =>
          // message id only goes to sender
          val messageIdO = Option.when(registeredMember.memberId == sender)(messageId)
          val groupRecipients = batch.allRecipients.collect { case x: GroupRecipient =>
            x
          }
          for {
            resolvedGroupAddresses <- {
              groupRecipients match {
                case x if x.isEmpty =>
                  // an optimization in case there are no group addresses
                  FutureUnlessShutdown.pure(Map.empty[GroupRecipient, Set[Member]])
                case x if x.sizeCompare(1) == 0 && x.contains(AllMembersOfSynchronizer) =>
                  // an optimization to avoid group address resolution on topology txs
                  FutureUnlessShutdown.pure(
                    Map[GroupRecipient, Set[Member]](AllMembersOfSynchronizer -> Set(member))
                  )
                case _ =>
                  for {
                    topologySnapshot <- topologySnapshotO.fold(
                      SyncCryptoClient
                        .getSnapshotForTimestamp(
                          syncCryptoApi,
                          timestamp,
                          topologyClientTimestampBeforeO,
                          protocolVersion,
                        )
                        .map(_.ipsSnapshot)
                    )(FutureUnlessShutdown.pure)
                    resolvedGroupAddresses <- GroupAddressResolver.resolveGroupsToMembers(
                      groupRecipients,
                      topologySnapshot,
                    )
                  } yield resolvedGroupAddresses
              }
            }
            memberGroupRecipients = resolvedGroupAddresses.collect {
              case (groupRecipient, groupMembers) if groupMembers.contains(member) => groupRecipient
            }.toSet
          } yield {
            val filteredBatch = Batch.filterClosedEnvelopesFor(batch, member, memberGroupRecipients)
            Deliver.create[ClosedEnvelope](
              counter,
              timestamp,
              synchronizerId,
              messageIdO,
              filteredBatch,
              topologyTimestampO,
              protocolVersion,
              // deliver events should only retain the traffic state for the sender's subscription
              trafficReceiptForNonSequencerSender(sender, trafficReceiptO),
            )
          }

        case ReceiptStoreEvent(
              sender,
              messageId,
              topologyTimestampO,
              _traceContext,
              trafficReceiptO,
            ) =>
          FutureUnlessShutdown.pure(
            Deliver.create[ClosedEnvelope](
              counter,
              timestamp,
              synchronizerId,
              Some(messageId),
              emptyBatch,
              topologyTimestampO,
              protocolVersion,
              trafficReceiptForNonSequencerSender(sender, trafficReceiptO),
            )
          )
        case DeliverErrorStoreEvent(sender, messageId, error, _traceContext, trafficReceiptO) =>
          val status = DeliverErrorStoreEvent
            .fromByteString(error, protocolVersion)
            .valueOr(err => throw new DbDeserializationException(err.toString))
          FutureUnlessShutdown.pure(
            DeliverError.create(
              counter,
              timestamp,
              synchronizerId,
              messageId,
              status,
              protocolVersion,
              trafficReceiptForNonSequencerSender(sender, trafficReceiptO),
            )
          )
      }
    }
  }

  /** Update the read state to start from the closest counter checkpoint if available */
  private def startFromClosestCounterCheckpoint(
      readState: ReadState,
      requestedCounter: SequencerCounter,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ReadState] =
    for {
      closestCheckpoint <- store.fetchClosestCheckpointBefore(
        readState.memberId,
        requestedCounter,
      )
    } yield {
      val startText = closestCheckpoint.fold("the beginning")(_.toString)
      logger.debug(
        s"Subscription for ${readState.member} at $requestedCounter will start from $startText"
      )
      closestCheckpoint.fold(readState)(readState.startFromCheckpoint)
    }
}

object SequencerReader {

  /** State to keep track of when serving a read subscription */
  private[SequencerReader] final case class ReadState(
      member: Member,
      memberId: SequencerMemberId,
      nextReadTimestamp: CantonTimestamp,
      latestTopologyClientRecipientTimestamp: Option[CantonTimestamp],
      lastBatchWasFull: Boolean = false,
      nextCounterAccumulator: SequencerCounter = SequencerCounter.Genesis,
  ) extends PrettyPrinting {

    def changeString(previous: ReadState): Option[String] = {
      def build[T](a: T, b: T, name: String): Option[String] =
        Option.when(a != b)(s"$name=$a (from $b)")
      val items = Seq(
        build(nextReadTimestamp, previous.nextReadTimestamp, "nextReadTs"),
        build(nextCounterAccumulator, previous.nextCounterAccumulator, "nextCounterAcc"),
        build(lastBatchWasFull, previous.lastBatchWasFull, "lastBatchWasFull"),
      ).flatten
      if (items.nonEmpty) {
        Some("New state is: " + items.mkString(", "))
      } else None
    }

    /** Update the state after reading a new page of results */
    def update(
        readEvents: ReadEvents,
        batchSize: Int,
    ): ReadState =
      copy(
        // increment the counter by the number of events we've now processed
        nextCounterAccumulator = nextCounterAccumulator + readEvents.events.size.toLong,
        // set the timestamp to next timestamp from the read events or keep the current timestamp if we got no results
        nextReadTimestamp = readEvents.nextTimestamp
          .getOrElse(nextReadTimestamp),
        // did we receive a full batch of events on this update
        lastBatchWasFull = readEvents.events.sizeCompare(batchSize) == 0,
      )

    /** Apply a previously recorded counter checkpoint so that we don't have to start from 0 on
      * every subscription
      */
    def startFromCheckpoint(checkpoint: CounterCheckpoint): ReadState =
      // with this checkpoint we'll start reading from this timestamp and as reads are not inclusive we'll receive the next event after this checkpoint first
      copy(
        nextCounterAccumulator = checkpoint.counter + 1,
        nextReadTimestamp = checkpoint.timestamp,
        latestTopologyClientRecipientTimestamp = checkpoint.latestTopologyClientTimestamp,
      )

    override protected def pretty: Pretty[ReadState] = prettyOfClass(
      param("member", _.member),
      param("memberId", _.memberId),
      param("nextReadTimestamp", _.nextReadTimestamp),
      param("latestTopologyClientRecipientTimestamp", _.latestTopologyClientRecipientTimestamp),
      param("lastBatchWasFull", _.lastBatchWasFull),
      param("nextCounterAccumulator", _.nextCounterAccumulator),
    )
  }

  private[SequencerReader] object ReadState {
    def initial(
        member: Member,
        registeredMember: RegisteredMember,
        latestTopologyClientRecipientTimestamp: CantonTimestamp,
    ): ReadState =
      ReadState(
        member = member,
        memberId = registeredMember.memberId,
        nextReadTimestamp = registeredMember.registeredFrom,
        latestTopologyClientRecipientTimestamp = Some(latestTopologyClientRecipientTimestamp),
      )
  }

  private[SequencerReader] final case class UnsignedEventData(
      event: SequencedEvent[ClosedEnvelope],
      signingSnapshotO: Option[SyncCryptoApi],
      previousTopologyClientTimestamp: Option[CantonTimestamp],
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      eventTraceContext: TraceContext,
  )
}
