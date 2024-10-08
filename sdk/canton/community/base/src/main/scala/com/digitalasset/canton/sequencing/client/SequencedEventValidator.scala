// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.Monad
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.flatMap.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{
  HashPurpose,
  SignatureCheckError,
  SyncCryptoApi,
  SyncCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.protocol.DynamicDomainParametersWithValidity
import com.digitalasset.canton.sequencing.client.SequencedEventValidationError.UpstreamSubscriptionError
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEvent}
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, PossiblyIgnoredSerializedEvent}
import com.digitalasset.canton.store.SequencedEventStore.IgnoredSequencedEvent
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.PekkoUtil.WithKillSwitch
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

sealed trait SequencedEventValidationError[+E] extends Product with Serializable with PrettyPrinting
object SequencedEventValidationError {
  final case class UpstreamSubscriptionError[+E: Pretty](error: E)
      extends SequencedEventValidationError[E] {
    override protected def pretty: Pretty[this.type] = prettyOfParam(_.error)
  }
  final case class BadDomainId(expected: DomainId, received: DomainId)
      extends SequencedEventValidationError[Nothing] {
    override protected def pretty: Pretty[BadDomainId] = prettyOfClass(
      param("expected", _.expected),
      param("received", _.received),
    )
  }
  final case class DecreasingSequencerCounter(
      newCounter: SequencerCounter,
      oldCounter: SequencerCounter,
  ) extends SequencedEventValidationError[Nothing] {
    override protected def pretty: Pretty[DecreasingSequencerCounter] = prettyOfClass(
      param("new counter", _.newCounter),
      param("old counter", _.oldCounter),
    )
  }
  final case class GapInSequencerCounter(newCounter: SequencerCounter, oldCounter: SequencerCounter)
      extends SequencedEventValidationError[Nothing] {
    override protected def pretty: Pretty[GapInSequencerCounter] = prettyOfClass(
      param("new counter", _.newCounter),
      param("old counter", _.oldCounter),
    )
  }
  final case class NonIncreasingTimestamp(
      newTimestamp: CantonTimestamp,
      newCounter: SequencerCounter,
      oldTimestamp: CantonTimestamp,
      oldCounter: SequencerCounter,
  ) extends SequencedEventValidationError[Nothing] {
    override protected def pretty: Pretty[NonIncreasingTimestamp] = prettyOfClass(
      param("new timestamp", _.newTimestamp),
      param("new counter", _.newCounter),
      param("old timestamp", _.oldTimestamp),
      param("old counter", _.oldCounter),
    )
  }
  final case class ForkHappened(
      counter: SequencerCounter,
      suppliedEvent: SequencedEvent[ClosedEnvelope],
      expectedEvent: Option[SequencedEvent[ClosedEnvelope]],
  )(implicit
      val loggingContext: ErrorLoggingContext
  ) extends CantonError.Impl(
        cause =
          "The sequencer responded with a different message for the same counter / timestamp, which means the sequencer forked."
      )(ResilientSequencerSubscription.ForkHappened)
      with SequencedEventValidationError[Nothing]
      with PrettyPrinting {
    override protected def pretty: Pretty[ForkHappened] = prettyOfClass(
      param("counter", _.counter),
      param("supplied event", _.suppliedEvent),
      paramIfDefined("expected event", _.expectedEvent),
    )
  }
  final case class SignatureInvalid(
      sequencedTimestamp: CantonTimestamp,
      usedTimestamp: CantonTimestamp,
      error: SignatureCheckError,
  ) extends SequencedEventValidationError[Nothing] {
    override protected def pretty: Pretty[SignatureInvalid] = prettyOfClass(
      unnamedParam(_.error),
      param("sequenced timestamp", _.sequencedTimestamp),
      param("used timestamp", _.usedTimestamp),
    )
  }
  final case class InvalidTopologyTimestamp(
      sequencedTimestamp: CantonTimestamp,
      declaredTopologyTimestamp: CantonTimestamp,
      reason: SequencedEventValidator.TopologyTimestampVerificationError,
  ) extends SequencedEventValidationError[Nothing] {
    override protected def pretty: Pretty[InvalidTopologyTimestamp] = prettyOfClass(
      param("sequenced timestamp", _.sequencedTimestamp),
      param("declared topology timestamp", _.declaredTopologyTimestamp),
      param("reason", _.reason),
    )
  }
  final case class TimestampOfSigningKeyNotAllowed(
      sequencedTimestamp: CantonTimestamp,
      declaredSigningKeyTimestamp: CantonTimestamp,
  ) extends SequencedEventValidationError[Nothing] {
    override protected def pretty: Pretty[TimestampOfSigningKeyNotAllowed] = prettyOfClass(
      param("sequenced timestamp", _.sequencedTimestamp),
      param("decalred signing key timestamp", _.declaredSigningKeyTimestamp),
    )
  }
}

/** Validate whether a received event is valid for processing. */
trait SequencedEventValidator extends AutoCloseable {

  /** Validates that the supplied event is suitable for processing from the prior event.
    * If the event is successfully validated it becomes the event that the event
    * in a following call will be validated against. We currently assume this is safe to do as if the event fails to be
    * handled by the application then the sequencer client will halt and will need recreating to restart event processing.
    */
  def validate(
      priorEvent: Option[PossiblyIgnoredSerializedEvent],
      event: OrdinarySerializedEvent,
      sequencerId: SequencerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit]

  /** Validates a sequenced event when we reconnect against the prior event supplied to [[SequencedEventValidatorFactory.create]] */
  def validateOnReconnect(
      priorEvent: Option[PossiblyIgnoredSerializedEvent],
      reconnectEvent: OrdinarySerializedEvent,
      sequencerId: SequencerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit]

  /** Add event validation to the given [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionPekko]].
    * Stuttering is interpreted as reconnection and validated accordingly.
    *
    * The returned [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionPekko]] completes after the first
    * event validation failure or the first subscription error. It does not stutter any more.
    *
    * @param priorReconnectEvent The sequenced event at which the reconnection happens.
    *                            If [[scala.Some$]], the first received event must be the same
    */
  def validatePekko[E: Pretty](
      subscription: SequencerSubscriptionPekko[E],
      priorReconnectEvent: Option[OrdinarySerializedEvent],
      sequencerId: SequencerId,
  )(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[SequencedEventValidationError[E]]
}

object SequencedEventValidator extends HasLoggerName {

  /** Do not validate sequenced events */
  private case object NoValidation extends SequencedEventValidator {
    override def validate(
        priorEvent: Option[PossiblyIgnoredSerializedEvent],
        event: OrdinarySerializedEvent,
        sequencerId: SequencerId,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] =
      EitherT(FutureUnlessShutdown.pure(Either.right(())))
    override def validateOnReconnect(
        priorEvent: Option[PossiblyIgnoredSerializedEvent],
        reconnectEvent: OrdinarySerializedEvent,
        sequencerId: SequencerId,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] =
      validate(priorEvent, reconnectEvent, sequencerId)

    override def validatePekko[E: Pretty](
        subscription: SequencerSubscriptionPekko[E],
        priorReconnectEvent: Option[OrdinarySerializedEvent],
        sequencerId: SequencerId,
    )(implicit
        traceContext: TraceContext
    ): SequencerSubscriptionPekko[SequencedEventValidationError[E]] =
      SequencerSubscriptionPekko(
        subscription.source.map(_.map(_.leftMap(UpstreamSubscriptionError(_)))),
        subscription.health,
      )

    override def close(): Unit = ()
  }

  /** Do not validate sequenced events.
    * Only use it in case of a programming error and the need to unblock a deployment or
    * if you blindly trust the sequencer.
    *
    * @param warn whether to log a warning when used
    */
  def noValidation(
      domainId: DomainId,
      warn: Boolean = true,
  )(implicit
      loggingContext: NamedLoggingContext
  ): SequencedEventValidator = {
    if (warn) {
      loggingContext.warn(
        s"You have opted to skip event validation for domain $domainId. You should not do this unless you know what you are doing."
      )
    }
    NoValidation
  }

  /** Validates the requested topology timestamp against the sequencing timestamp and the
    * [[com.digitalasset.canton.protocol.DynamicDomainParameters.sequencerTopologyTimestampTolerance]]
    * of the domain parameters valid at the requested topology timestamp.
    *
    * @param latestTopologyClientTimestamp The timestamp of an earlier event sent to the topology client
    *                                      such that no topology update has happened
    *                                      between this timestamp (exclusive) and the sequencing timestamp (exclusive).
    * @param warnIfApproximate             Whether to emit a warning if an approximate topology snapshot is used
    * @return [[scala.Left$]] if the topology timestamp is after the sequencing timestamp or the sequencing timestamp
    *         is after the topology timestamp by more than the
    *         [[com.digitalasset.canton.protocol.DynamicDomainParameters.sequencerTopologyTimestampTolerance]] valid at the topology timestamp.
    *         [[scala.Right$]] the topology snapshot that can be used for signing the event
    *         and verifying the signature on the event;
    */
  def validateTopologyTimestamp(
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      topologyTimestamp: CantonTimestamp,
      sequencingTimestamp: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
      warnIfApproximate: Boolean,
      getTolerance: DynamicDomainParametersWithValidity => NonNegativeFiniteDuration,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, TopologyTimestampVerificationError, SyncCryptoApi] =
    validateTopologyTimestampInternal(
      syncCryptoApi,
      topologyTimestamp,
      sequencingTimestamp,
      latestTopologyClientTimestamp,
      protocolVersion,
      warnIfApproximate,
      getTolerance,
    )(
      SyncCryptoClient.getSnapshotForTimestamp _,
      (topology, traceContext) => topology.findDynamicDomainParameters()(traceContext),
    )

  def validateTopologyTimestampUS(
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      topologyTimestamp: CantonTimestamp,
      sequencingTimestamp: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
      warnIfApproximate: Boolean,
      getTolerance: DynamicDomainParametersWithValidity => NonNegativeFiniteDuration,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, TopologyTimestampVerificationError, SyncCryptoApi] =
    validateTopologyTimestampInternal(
      syncCryptoApi,
      topologyTimestamp,
      sequencingTimestamp,
      latestTopologyClientTimestamp,
      protocolVersion,
      warnIfApproximate,
      getTolerance,
    )(
      SyncCryptoClient.getSnapshotForTimestampUS _,
      (topology, traceContext) =>
        closeContext.context.performUnlessClosingF("get-dynamic-parameters")(
          topology.findDynamicDomainParameters()(traceContext)
        )(executionContext, traceContext),
    )

  // Base version of validateSigningTimestamp abstracting over the effect type to allow for
  // a `Future` and `FutureUnlessShutdown` version. Once we migrate all usages to the US version, this abstraction
  // should not be needed anymore
  private def validateTopologyTimestampInternal[F[_]: Monad](
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      topologyTimestamp: CantonTimestamp,
      sequencingTimestamp: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
      warnIfApproximate: Boolean,
      getTolerance: DynamicDomainParametersWithValidity => NonNegativeFiniteDuration,
  )(
      getSnapshotF: (
          SyncCryptoClient[SyncCryptoApi],
          CantonTimestamp,
          Option[CantonTimestamp],
          ProtocolVersion,
          Boolean,
      ) => F[SyncCryptoApi],
      getDynamicDomainParameters: (
          TopologySnapshot,
          TraceContext,
      ) => F[Either[String, DynamicDomainParametersWithValidity]],
  )(implicit
      loggingContext: NamedLoggingContext
  ): EitherT[F, TopologyTimestampVerificationError, SyncCryptoApi] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext

    def snapshotF: F[SyncCryptoApi] = getSnapshotF(
      syncCryptoApi,
      // As we use topologyTimestamp here (as opposed to sequencingTimestamp),
      // a valid topologyTimestamp can be used until topologyTimestamp + tolerance.
      // So a change of tolerance does not negatively impact pending requests.
      topologyTimestamp,
      latestTopologyClientTimestamp,
      protocolVersion,
      warnIfApproximate,
    )

    def validateWithSnapshot(
        snapshot: SyncCryptoApi
    ): F[Either[TopologyTimestampVerificationError, SyncCryptoApi]] =
      getDynamicDomainParameters(snapshot.ipsSnapshot, traceContext)
        .map { dynamicDomainParametersE =>
          for {
            dynamicDomainParameters <- dynamicDomainParametersE.leftMap(
              NoDynamicDomainParameters.apply
            )
            tolerance = getTolerance(dynamicDomainParameters)
            withinSigningTolerance = {
              import scala.Ordered.orderingToOrdered
              tolerance.unwrap >= sequencingTimestamp - topologyTimestamp
            }
            _ <- Either.cond(withinSigningTolerance, (), TopologyTimestampTooOld(tolerance))
          } yield snapshot
        }

    if (topologyTimestamp > sequencingTimestamp) {
      EitherT.leftT[F, SyncCryptoApi](TopologyTimestampAfterSequencingTime)
    } else if (topologyTimestamp == sequencingTimestamp) {
      // If the signing timestamp is the same as the sequencing timestamp,
      // we don't need to check the tolerance because it is always non-negative.
      EitherT.right[TopologyTimestampVerificationError](snapshotF)
    } else {
      EitherT(snapshotF.flatMap(validateWithSnapshot))
    }
  }

  sealed trait TopologyTimestampVerificationError
      extends Product
      with Serializable
      with PrettyPrinting
  case object TopologyTimestampAfterSequencingTime extends TopologyTimestampVerificationError {
    override protected def pretty: Pretty[TopologyTimestampAfterSequencingTime] =
      prettyOfObject[TopologyTimestampAfterSequencingTime]
  }
  type TopologyTimestampAfterSequencingTime = TopologyTimestampAfterSequencingTime.type

  final case class TopologyTimestampTooOld(tolerance: NonNegativeFiniteDuration)
      extends TopologyTimestampVerificationError {
    override protected def pretty: Pretty[TopologyTimestampTooOld] = prettyOfClass(
      param("tolerance", _.tolerance)
    )
  }

  final case class NoDynamicDomainParameters(error: String)
      extends TopologyTimestampVerificationError {
    override protected def pretty: Pretty[NoDynamicDomainParameters] = prettyOfClass(
      param("error", _.error.unquoted)
    )
  }
}

trait SequencedEventValidatorFactory {

  /** Creates a new [[SequencedEventValidator]] to be used for a subscription with the given parameters.
    */
  def create(loggerFactory: NamedLoggerFactory)(implicit
      traceContext: TraceContext
  ): SequencedEventValidator
}

object SequencedEventValidatorFactory {

  /** Do not validate sequenced events.
    * Only use it in case of a programming error and the need to unblock a deployment or
    * if you blindly trust the sequencer.
    *
    * @param warn whether to log a warning
    */
  def noValidation(
      domainId: DomainId,
      warn: Boolean = true,
  ): SequencedEventValidatorFactory = new SequencedEventValidatorFactory {
    override def create(loggerFactory: NamedLoggerFactory)(implicit
        traceContext: TraceContext
    ): SequencedEventValidator =
      SequencedEventValidator.noValidation(domainId, warn)(
        NamedLoggingContext(loggerFactory, traceContext)
      )
  }
}

/** Validate whether a received event is valid for processing. */
class SequencedEventValidatorImpl(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
    protected val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit executionContext: ExecutionContext)
    extends SequencedEventValidator
    with FlagCloseable
    with HasCloseContext
    with NamedLogging {

  import SequencedEventValidationError.*
  import SequencedEventValidatorImpl.*

  /** Validates that the supplied event is suitable for processing from the prior event.
    * Currently the signature not being valid is not considered an error but its validity is returned to the caller
    * to allow them to choose what to do with the event.
    * If the event is successfully validated (regardless of the signature check) it becomes the event that the event
    * in a following call will be validated against. We currently assume this is safe to do as if the event fails to be
    * handled by the application then the sequencer client will halt and will need recreating to restart event processing.
    * This method must not be called concurrently as it will corrupt the prior event state.
    */
  override def validate(
      priorEventO: Option[PossiblyIgnoredSerializedEvent],
      event: OrdinarySerializedEvent,
      sequencerId: SequencerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] = {
    val oldCounter = priorEventO.fold(SequencerCounter.Genesis - 1L)(_.counter)
    val newCounter = event.counter
    val newTimestamp = event.timestamp

    def checkCounterIncreases: ValidationResult =
      Either.cond(
        newCounter == oldCounter + 1,
        (),
        if (newCounter < oldCounter) DecreasingSequencerCounter(newCounter, oldCounter)
        else GapInSequencerCounter(newCounter, oldCounter),
      )

    def checkTimestampIncreases: ValidationResult =
      priorEventO.traverse_ { prior =>
        val oldTimestamp = prior.timestamp
        Either.cond(
          newTimestamp > oldTimestamp,
          (),
          NonIncreasingTimestamp(newTimestamp, newCounter, oldTimestamp, oldCounter),
        )
      }

    // TODO(M99): dishonest sequencer: Check that the node is listed as a recipient on all envelopes in the batch

    logger.debug(s"Validating event $event")

    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Seq(
          checkCounterIncreases,
          checkDomainId(event),
          checkTimestampIncreases,
        ).sequence_
      )
      _ = logger.debug(
        s"Successfully checked domain ID (${event.signedEvent.content.domainId}), " +
          s"increasing counter (old = $oldCounter, new = $newCounter) " +
          s"and increasing timestamp (old = ${priorEventO.map(_.timestamp)}, new = $newTimestamp)"
      )
      // Verify the signature only if we know of a prior event.
      // Otherwise, this is a fresh subscription and we will get the topology state with the first transaction
      // TODO(#4933) Upon a fresh subscription, retrieve the keys via the topology API and validate immediately or
      //  validate the signature after processing the initial event
      _ <- verifySignature(priorEventO, event, sequencerId, protocolVersion)
      _ = logger.debug("Successfully verified signature")
    } yield ()
  }

  override def validateOnReconnect(
      priorEvent0: Option[PossiblyIgnoredSerializedEvent],
      reconnectEvent: OrdinarySerializedEvent,
      sequencerId: SequencerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] = {
    implicit val traceContext: TraceContext = reconnectEvent.traceContext
    val priorEvent = priorEvent0.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(
          s"No prior event known even though the sequencer client resubscribes to $sequencerId at sequencer counter ${reconnectEvent.counter}"
        )
      )
    )
    val checkFork: Either[SequencedEventValidationError[Nothing], Unit] = priorEvent match {
      case ordinaryPrior: OrdinarySerializedEvent =>
        val oldSequencedEvent = ordinaryPrior.signedEvent.content
        val newSequencedEvent = reconnectEvent.signedEvent.content
        // We compare the contents of the `SequencedEvent` rather than their serialization
        // because the SequencerReader serializes the `SequencedEvent` afresh upon each resubscription
        // and the serialization may therefore differ from time to time. This is fine for auditability
        // because the sequencer also delivers a new signature on the new serialization.
        Either.cond(
          oldSequencedEvent == newSequencedEvent,
          (),
          ForkHappened(oldSequencedEvent.counter, newSequencedEvent, Some(oldSequencedEvent)),
        )
      case ignored: IgnoredSequencedEvent[ClosedEnvelope] =>
        // If the event should be ignored, we nevertheless check the counter
        // We merely check timestamp monotonicity, but not the exact timestamp
        // because when we ignore unsequenced events, we assign them the least possible timestamp.
        Either.cond(
          ignored.counter == reconnectEvent.counter && ignored.timestamp <= reconnectEvent.timestamp,
          (),
          ForkHappened(
            ignored.counter,
            reconnectEvent.signedEvent.content,
            ignored.underlying.map(_.content),
          ),
        )
    }

    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Seq(
          checkDomainId(reconnectEvent),
          checkFork,
        ).sequence_
      )
      _ <- verifySignature(Some(priorEvent), reconnectEvent, sequencerId, protocolVersion)
    } yield ()
    // do not update the priorEvent because if it was ignored, then it was ignored for a reason.
  }

  private def checkDomainId(event: OrdinarySerializedEvent): ValidationResult = {
    val receivedDomainId = event.signedEvent.content.domainId
    Either.cond(receivedDomainId == domainId, (), BadDomainId(domainId, receivedDomainId))
  }

  @VisibleForTesting
  protected def verifySignature(
      priorEventO: Option[PossiblyIgnoredSerializedEvent],
      event: OrdinarySerializedEvent,
      sequencerId: SequencerId,
      protocolVersion: ProtocolVersion,
  ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] = {
    implicit val traceContext: TraceContext = event.traceContext
    if (event.counter == SequencerCounter.Genesis) {
      // TODO(#4933) This is a fresh subscription. Either fetch the domain keys via a future sequencer API and validate the signature
      //  or wait until the topology processor has processed the topology information in the first message and then validate the signature.
      logger.info(
        s"Skipping signature verification of the first sequenced event due to a fresh subscription from $sequencerId"
      )
      // The first sequenced event addressed to a member must not specify a signing key timestamp because
      // the member will only be able to compute snapshots for the current topology state and later.
      EitherT.fromEither[FutureUnlessShutdown](checkNoTimestampOfSigningKey(event))
    } else {
      val signingTs = event.signedEvent.content.timestampOfSigningKey
      for {
        _ <- EitherT.fromEither[FutureUnlessShutdown](checkNoTimestampOfSigningKey(event))
        _ = logger.debug("Successfully checked that there's no timestamp of signing key")
        snapshot <- SequencedEventValidator
          .validateTopologyTimestampUS(
            syncCryptoApi,
            signingTs,
            event.timestamp,
            lastTopologyClientTimestamp(priorEventO),
            protocolVersion,
            warnIfApproximate = priorEventO.nonEmpty,
            _.sequencerTopologyTimestampTolerance,
          )
          .leftMap(InvalidTopologyTimestamp(event.timestamp, signingTs, _))
        _ = logger.debug(s"Successfully validated the event topology timestamp ${event.timestamp}")
        _ <- event.signedEvent
          .verifySignature(snapshot, sequencerId, HashPurpose.SequencedEventSignature)
          .leftMap[SequencedEventValidationError[Nothing]](
            SignatureInvalid(event.timestamp, signingTs, _)
          )
          .mapK(FutureUnlessShutdown.outcomeK)
      } yield ()
    }
  }

  /** The timestamp of signing key is always derived from the timestamps in the [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]],
    * so it must never be set as [[com.digitalasset.canton.sequencing.protocol.SignedContent.timestampOfSigningKey]]
    */
  private def checkNoTimestampOfSigningKey(event: OrdinarySerializedEvent): ValidationResult =
    event.signedEvent.timestampOfSigningKey
      .toLeft(())
      .leftMap(TimestampOfSigningKeyNotAllowed(event.timestamp, _))

  override def validatePekko[E: Pretty](
      subscription: SequencerSubscriptionPekko[E],
      priorReconnectEvent: Option[OrdinarySerializedEvent],
      sequencerId: SequencerId,
  )(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[SequencedEventValidationError[E]] = {
    def performValidation(
        rememberedAndCurrent: NonEmpty[Seq[WithKillSwitch[Either[E, OrdinarySerializedEvent]]]]
    ): FutureUnlessShutdown[WithKillSwitch[
      // None if the element should not be emitted
      Option[Either[SequencedEventValidationError[E], OrdinarySerializedEvent]]
    ]] =
      rememberedAndCurrent.last1.traverse {
        case Left(err) => FutureUnlessShutdown.pure(Some(Left(UpstreamSubscriptionError(err))))
        case Right(current) =>
          val validationEF =
            if (rememberedAndCurrent.sizeIs <= 1)
              validateOnReconnect(priorReconnectEvent, current, sequencerId).value.map(
                _.traverse((_: Unit) => None)
              )
            else {
              val previousEvent = rememberedAndCurrent.head1.value.valueOr { previousErr =>
                implicit val traceContext: TraceContext = current.traceContext
                ErrorUtil.invalidState(
                  s"Subscription for sequencer $sequencerId delivered an event at counter ${current.counter} after having previously signalled the error $previousErr"
                )
              }
              // SequencerSubscriptions may stutter on reconnect, e.g., inside a resilient sequencer subscription
              val previousEventId = (previousEvent.counter, previousEvent.timestamp)
              val currentEventId = (current.counter, current.timestamp)
              val stutter = previousEventId == currentEventId
              if (stutter)
                validateOnReconnect(Some(previousEvent), current, sequencerId).value
                  .map(_.traverse((_: Unit) => None))
              else
                validate(Some(previousEvent), current, sequencerId).value
                  .map(_.traverse((_: Unit) => Option(current)))
            }
          validationEF
      }

    val validatedSource = subscription.source
      .remember(NonNegativeInt.one)
      .statefulMapAsyncUS(false) { (failedPreviously, event) =>
        // Do not start the validation of the next event if the previous one failed.
        // Otherwise, we may deadlock on the topology snapshot because the event with the failed validation
        // may never reach the topology processor.
        if (failedPreviously)
          FutureUnlessShutdown.pure(failedPreviously -> event.last1.map(_ => None))
        else
          performValidation(event).map { validation =>
            val failed = validation.value.exists(_.isLeft)
            failed -> validation
          }
      }
      // Filter out the stuttering
      .mapConcat {
        case UnlessShutdown.AbortedDueToShutdown =>
          // TODO(#13789) should we pull a kill switch here?
          None
        case UnlessShutdown.Outcome(result) => result.sequence
      }
      .takeUntilThenDrain(_.isLeft)
    SequencerSubscriptionPekko(validatedSource, subscription.health)
  }
}

object SequencedEventValidatorImpl {
  private[SequencedEventValidatorImpl] type ValidationResult =
    Either[SequencedEventValidationError[Nothing], Unit]

  /** The sequencer client assumes that the topology processor is ticked for every event proecessed,
    * even if the event is a [[com.digitalasset.canton.store.SequencedEventStore.IgnoredSequencedEvent]].
    * This is why [[com.digitalasset.canton.sequencing.handlers.DiscardIgnoredEvents]]
    * must not be used in application handlers on nodes that support ignoring events.
    */
  private[SequencedEventValidatorImpl] def lastTopologyClientTimestamp(
      priorEvent: Option[PossiblyIgnoredSerializedEvent]
  ): Option[CantonTimestamp] =
    priorEvent.map(_.timestamp)
}
