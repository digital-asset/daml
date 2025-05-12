// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.{EitherT, OptionT}
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.order.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config
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
import com.digitalasset.canton.sequencing.{GroupAddressResolver, SequencedSerializedEvent}
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.synchronizer.sequencer.SequencerReader.ReadState
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.synchronizer.sequencer.store.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.WithKillSwitch
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Source}
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.ExecutionContext

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
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  def readV2(member: Member, requestedTimestampInclusive: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.SequencedEventSource] =
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

      _ = logger.debug(
        s"Topology processor at: ${syncCryptoApi.approximateTimestamp}"
      )

      safeWatermarkTimestampO <- EitherT.right(store.fetchWatermark(0)).map(_.map(_.timestamp))
      _ = logger.debug(
        s"Current safe watermark is $safeWatermarkTimestampO"
      )

      // It can happen that a member switching between sequencers runs into a sequencer that is catching up.
      // In this situation, the sequencer has to wait for the watermark to catch up to the requested timestamp.
      // To address this we start reading from the watermark timestamp itself, and use the dropWhile filter
      // of the `reader.from` to only release events that are at or after the requested timestamp to the subscriber.
      readFromTimestampInclusive =
        if (safeWatermarkTimestampO < requestedTimestampInclusive) {
          logger.debug(
            s"Safe watermark $safeWatermarkTimestampO is before the requested timestamp. Will commence reading from the safe watermark and skip events until requested timestamp $requestedTimestampInclusive (inclusive)."
          )
          safeWatermarkTimestampO
        } else {
          requestedTimestampInclusive
        }

      latestTopologyClientRecipientTimestamp <- EitherT.right(
        readFromTimestampInclusive
          .flatTraverse { timestamp =>
            store.latestTopologyClientRecipientTimestamp(
              member = member,
              timestampExclusive =
                timestamp, // this is correct as we query for latest timestamp before `timestampInclusive`
            )
          }
          .map(
            _.getOrElse(
              memberOnboardingTxSequencingTime
            )
          )
      )

      previousEventTimestamp <- EitherT.right(readFromTimestampInclusive.flatTraverse { timestamp =>
        store.previousEventTimestamp(
          registeredMember.memberId,
          timestampExclusive =
            timestamp, // this is correct as we query for latest timestamp before `timestampInclusive`
        )
      })
      _ = logger.debug(
        s"New subscription for $member will start with previous event timestamp = $previousEventTimestamp " +
          s"and latest topology client timestamp = $latestTopologyClientRecipientTimestamp"
      )

      // validate we are in the bounds of the data that this sequencer can serve
      lowerBoundExclusiveO <- EitherT.right(store.fetchLowerBound())
      _ <- EitherT
        .cond[FutureUnlessShutdown](
          (requestedTimestampInclusive, lowerBoundExclusiveO) match {
            // Reading from the beginning, with no lower bound
            case (None, None) => true
            // Reading from the beginning, with a lower bound present
            case (None, Some((lowerBoundExclusive, _))) =>
              // require that the member is registered above the lower bound
              // unless it's this sequencer's own self-subscription from the beginning
              registeredMember.registeredFrom > lowerBoundExclusive || topologyClientMember == member
            // Reading from a specified timestamp, with no lower bound
            case (Some(requestedTimestampInclusive), None) =>
              // require that the requested timestamp is above or at the member registration time
              requestedTimestampInclusive >= registeredMember.registeredFrom
            // Reading from a specified timestamp, with a lower bound present
            case (Some(requestedTimestampInclusive), Some((lowerBoundExclusive, _))) =>
              // require that the requested timestamp is above the lower bound
              // and above or at the member registration time
              requestedTimestampInclusive > lowerBoundExclusive &&
              requestedTimestampInclusive >= registeredMember.registeredFrom
          },
          (), {
            val lowerBoundText = lowerBoundExclusiveO
              .map { case (lowerBound, _) => lowerBound.toString }
              .getOrElse("epoch")
            val timestampText = readFromTimestampInclusive
              .map(timestamp => s"$timestamp (inclusive)")
              .getOrElse("the beginning")
            val errorMessage =
              show"Subscription for $member would require reading data from $timestampText, " +
                show"but this sequencer cannot serve timestamps at or before ${lowerBoundText.unquoted} " +
                show"or below the member's registration timestamp ${registeredMember.registeredFrom}."

            logger.error(errorMessage)
            CreateSubscriptionError
              .EventsUnavailableForTimestamp(readFromTimestampInclusive, errorMessage)
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
      reader.from(
        event => requestedTimestampInclusive.exists(event.unvalidatedEvent.timestamp < _),
        ReadState(
          member,
          registeredMember.memberId,
          // This is a "reading watermark" meaning that "we have read up to and including this timestamp",
          // so if we want to grab the event exactly at timestampInclusive, we do -1 here
          nextReadTimestamp = readFromTimestampInclusive
            .map(_.immediatePredecessor)
            .getOrElse(
              memberOnboardingTxSequencingTime
            ),
          nextPreviousEventTimestamp = previousEventTimestamp,
          latestTopologyClientRecipientTimestamp = latestTopologyClientRecipientTimestamp.some,
        ),
      )
    })

  private[SequencerReader] class EventsReader(
      member: Member,
      registeredMember: RegisteredMember,
      topologyClientMemberId: SequencerMemberId,
      override protected val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging {

    import SequencerReader.*

    private def unvalidatedEventsSourceFromReadState(initialReadState: ReadState)(implicit
        traceContext: TraceContext
    ): Source[(PreviousEventTimestamp, Sequenced[IdOrPayload]), NotUsed] =
      eventSignaller
        .readSignalsForMember(member, registeredMember.memberId)
        .via(
          FetchLatestEventsFlow[
            (PreviousEventTimestamp, Sequenced[IdOrPayload]),
            ReadState,
          ](
            initialReadState,
            state => fetchUnvalidatedEventsBatchFromReadState(state)(traceContext),
            (state, _) => !state.lastBatchWasFull,
          )
        )

    private def signValidatedEvent(
        unsignedEventData: UnsignedEventData
    ): EitherT[FutureUnlessShutdown, SequencedEventError, SequencedSerializedEvent] = {
      val UnsignedEventData(
        event,
        topologySnapshotO,
        previousTopologyClientTimestamp,
        latestTopologyClientTimestamp,
        eventTraceContext,
      ) = unsignedEventData
      implicit val traceContext: TraceContext = eventTraceContext
      logger.trace(
        s"Latest topology client timestamp for $member at sequencing timestamp ${event.timestamp} is $previousTopologyClientTimestamp / $latestTopologyClientTimestamp"
      )

      val res = for {
        signingSnapshot <- OptionT
          .fromOption[FutureUnlessShutdown](topologySnapshotO)
          .getOrElseF {
            val warnIfApproximate =
              event.previousTimestamp.nonEmpty // warn if we are not at genesis
            SyncCryptoClient.getSnapshotForTimestamp(
              syncCryptoApi,
              event.timestamp,
              previousTopologyClientTimestamp,
              protocolVersion,
              warnIfApproximate = warnIfApproximate,
            )
          }
        _ = logger.debug(
          s"Signing event with sequencing timestamp ${event.timestamp} for $member"
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
        previousTimestamp: PreviousEventTimestamp,
        unvalidatedEvent: Sequenced[P],
    ) {
      def mapEventPayload[Q](f: P => Q): ValidatedSnapshotWithEvent[Q] =
        ValidatedSnapshotWithEvent[Q](
          topologyClientTimestampBefore,
          snapshotOrError,
          previousTimestamp,
          unvalidatedEvent.map(f),
        )
    }

    def validateEvent(
        topologyClientTimestampBefore: Option[CantonTimestamp],
        sequenced: (PreviousEventTimestamp, Sequenced[IdOrPayload]),
    ): FutureUnlessShutdown[
      (TopologyClientTimestampAfter, ValidatedSnapshotWithEvent[IdOrPayload])
    ] = {
      val (previousTimestamp, unvalidatedEvent) = sequenced

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
              previousTimestamp,
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
              previousTimestamp,
              unvalidatedEvent,
            )
          )

      }
    }

    def generateEvent(
        snapshotWithEvent: ValidatedSnapshotWithEvent[Batch[ClosedEnvelope]]
    ): FutureUnlessShutdown[UnsignedEventData] = {
      implicit val traceContext = snapshotWithEvent.unvalidatedEvent.traceContext
      import snapshotWithEvent.{previousTimestamp, topologyClientTimestampBefore, unvalidatedEvent}

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
              previousTimestamp,
              unvalidatedEvent,
              None,
              topologyClientTimestampBefore,
            )(unvalidatedEvent.traceContext)
          validationSuccess(eventF, None)

        case Some(Right(topologySnapshot)) =>
          val eventF =
            mkSequencedEvent(
              previousTimestamp,
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
              s"The topology timestamp $topologyTimestamp must be before or at the sequencing timestamp ${unvalidatedEvent.timestamp} for event with sequencing timestamp ${unvalidatedEvent.timestamp} of member $member"
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
          // TODO(#25162): After counter removal, we don't need to prevent gaps in the sequencer counters,
          //  so we can drop the event instead of delivering an empty batch for other members
          val event = if (registeredMember.memberId == unvalidatedEvent.event.sender) {
            val error =
              SequencerErrors.TopologyTimestampTooEarly(
                topologyTimestamp,
                unvalidatedEvent.timestamp,
              )
            DeliverError.create(
              previousTimestamp,
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
              previousTimestamp,
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

    def from(
        dropWhile: ValidatedSnapshotWithEvent[IdOrPayload] => Boolean,
        initialReadState: ReadState,
    )(implicit
        traceContext: TraceContext
    ): Sequencer.SequencedEventSource = {
      val unvalidatedEventsSrc = unvalidatedEventsSourceFromReadState(initialReadState)
      val validatedEventSrc = unvalidatedEventsSrc.statefulMapAsyncUSAndDrain(
        initialReadState.latestTopologyClientRecipientTimestamp
      )(validateEvent)
      val eventsSource =
        validatedEventSrc
          // drop events we don't care about before fetching payloads
          .dropWhile(dropWhile)
          .viaMat(KillSwitches.single)(Keep.right)
          .injectKillSwitch(identity)
          .via(fetchPayloadsForEventsBatch())

      // TODO(#23857): With validated events here we will persist their validation status for re-use by other subscriptions.
      eventsSource
        .viaMat(KillSwitches.single) { case (killSwitch, _) =>
          (killSwitch, FutureUnlessShutdown.pure(Done))
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

    private def fetchUnvalidatedEventsBatchFromReadState(
        readState: ReadState
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[
      (ReadState, Seq[(PreviousEventTimestamp, Sequenced[IdOrPayload])])
    ] =
      for {
        readEvents <- store.readEvents(
          readState.memberId,
          readState.member,
          readState.nextReadTimestamp.some,
          config.readBatchSize,
        )
      } yield {
        val previousTimestamps = readState.nextPreviousEventTimestamp +: readEvents.events.view
          .dropRight(1)
          .map(_.timestamp.some)
        val eventsWithPreviousTimestamps = previousTimestamps.zip(readEvents.events).toSeq

        val newReadState = readState.update(readEvents, config.readBatchSize)
        if (newReadState.nextReadTimestamp < readState.nextReadTimestamp) {
          ErrorUtil.invalidState(
            s"Read state is going backwards in time: ${newReadState.changeString(readState)}"
          )
        }
        if (logger.underlying.isDebugEnabled) {
          newReadState.changeString(readState).foreach(logger.debug(_))
        }
        (newReadState, eventsWithPreviousTimestamps)
      }

    private def signEvent(
        event: SequencedEvent[ClosedEnvelope],
        topologySnapshot: SyncCryptoApi,
    )(implicit traceContext: TraceContext): EitherT[
      FutureUnlessShutdown,
      SequencerSubscriptionError.TombstoneEncountered.Error,
      SequencedSerializedEvent,
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
                  event.timestamp,
                  member,
                  topologySnapshot.ipsSnapshot.timestamp,
                )
              logger.warn(error.cause)
              error
            case err =>
              throw new IllegalStateException(s"Signing failed with an unexpected error: $err")
          }
      } yield SequencedEventWithTraceContext(signedEvent)(traceContext)

    private def trafficReceiptForNonSequencerSender(
        senderMemberId: SequencerMemberId,
        trafficReceiptO: Option[TrafficReceipt],
    ) =
      if (member.code == SequencerId.Code || registeredMember.memberId != senderMemberId) None
      else trafficReceiptO

    /** Takes our stored event and turns it back into a real sequenced event.
      */
    private def mkSequencedEvent(
        previousTimestamp: PreviousEventTimestamp,
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
              previousTimestamp,
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
              previousTimestamp,
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
              previousTimestamp,
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
}

object SequencerReader {

  type PreviousEventTimestamp = Option[CantonTimestamp]

  /** State to keep track of when serving a read subscription */
  private[SequencerReader] final case class ReadState(
      member: Member,
      memberId: SequencerMemberId,
      nextReadTimestamp: CantonTimestamp,
      latestTopologyClientRecipientTimestamp: Option[CantonTimestamp],
      lastBatchWasFull: Boolean = false,
      nextPreviousEventTimestamp: Option[CantonTimestamp] = None,
  ) extends PrettyPrinting {

    def changeString(previous: ReadState): Option[String] = {
      def build[T](a: T, b: T, name: String): Option[String] =
        Option.when(a != b)(s"$name=$a (from $b)")
      val items = Seq(
        build(nextReadTimestamp, previous.nextReadTimestamp, "nextReadTs"),
        build(
          nextPreviousEventTimestamp,
          previous.nextPreviousEventTimestamp,
          "nextPreviousEventTimestamp",
        ),
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
        // set the previous event timestamp to the last event we've read or keep the current one if we got no results
        nextPreviousEventTimestamp = readEvents.events.lastOption match {
          case Some(event) => Some(event.timestamp)
          case None => nextPreviousEventTimestamp
        },
        // set the timestamp to next timestamp from the read events or keep the current timestamp if we got no results
        nextReadTimestamp = readEvents.nextTimestamp
          .getOrElse(nextReadTimestamp),
        // did we receive a full batch of events on this update
        lastBatchWasFull = readEvents.events.sizeCompare(batchSize) == 0,
      )

    override protected def pretty: Pretty[ReadState] = prettyOfClass(
      param("member", _.member),
      param("memberId", _.memberId),
      param("nextReadTimestamp", _.nextReadTimestamp),
      param("latestTopologyClientRecipientTimestamp", _.latestTopologyClientRecipientTimestamp),
      param("lastBatchWasFull", _.lastBatchWasFull),
      param("nextPreviousEventTimestamp", _.nextPreviousEventTimestamp),
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
