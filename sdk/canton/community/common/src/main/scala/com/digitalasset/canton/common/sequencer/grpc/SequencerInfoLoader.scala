// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.sequencer.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.*
import com.digitalasset.canton.common.sequencer.SequencerConnectClient
import com.digitalasset.canton.common.sequencer.SequencerConnectClient.SynchronizerClientBootstrapInfo
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader.{
  LoadSequencerEndpointInformationResult,
  SequencerAggregatedInfo,
  SequencerInfoLoaderError,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnectionValidation,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, LoggerUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.{nowarn, tailrec}
import scala.concurrent.{ExecutionContextExecutor, Promise}

class SequencerInfoLoader(
    timeouts: ProcessingTimeout,
    traceContextPropagation: TracingConfig.Propagation,
    clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
    minimumProtocolVersion: Option[ProtocolVersion],
    dontWarnOnDeprecatedPV: Boolean,
    protected val loggerFactory: NamedLoggerFactory,
    sequencerInfoLoadParallelism: NonNegativeInt = NonNegativeInt.tryCreate(4),
)(implicit
    val ec: ExecutionContextExecutor,
    val materializer: Materializer,
    val tracer: Tracer,
) extends NamedLogging {

  private def sequencerConnectClientBuilder: SequencerConnectClient.Builder = {
    (synchronizerAlias: SynchronizerAlias, config: SequencerConnection) =>
      SequencerConnectClient(
        synchronizerAlias,
        config,
        timeouts,
        traceContextPropagation,
        loggerFactory,
      )
  }

  private def getBootstrapInfoSynchronizerParameters(
      synchronizerAlias: SynchronizerAlias,
      sequencerAlias: SequencerAlias,
      client: SequencerConnectClient,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SequencerInfoLoaderError,
    (SynchronizerClientBootstrapInfo, StaticSynchronizerParameters),
  ] = {

    logger.debug(s"Querying bootstrap information for synchronizer $synchronizerAlias")
    for {
      bootstrapInfo <- client
        .getSynchronizerClientBootstrapInfo(synchronizerAlias)
        .leftMap(SequencerInfoLoader.fromSequencerConnectClientError(synchronizerAlias))

      _ <- performHandshake(client, synchronizerAlias, sequencerAlias)

      synchronizerParameters <- client
        .getSynchronizerParameters(synchronizerAlias.unwrap)
        .leftMap(SequencerInfoLoader.fromSequencerConnectClientError(synchronizerAlias))

      _ = logger.info(
        s"Retrieved synchronizer information: bootstrap information is $bootstrapInfo and synchronizer parameters are $synchronizerParameters"
      )

    } yield (bootstrapInfo, synchronizerParameters)
  }

  private def getBootstrapInfoSynchronizerParametersWithRetry(
      synchronizerAlias: SynchronizerAlias,
      sequencerAlias: SequencerAlias,
      client: SequencerConnectClient,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[
    FutureUnlessShutdown,
    SequencerInfoLoaderError,
    (SynchronizerClientBootstrapInfo, StaticSynchronizerParameters),
  ] = {
    val retries = 10
    EitherT(
      retry
        .Pause(
          logger,
          performUnlessClosing = closeContext.context,
          maxRetries = retries,
          delay = timeouts.sequencerInfo.asFiniteApproximation.div(retries.toLong),
          operationName =
            s"${synchronizerAlias.toProtoPrimitive}/${sequencerAlias.toProtoPrimitive}: $functionFullName",
        )
        .unlessShutdown(
          getBootstrapInfoSynchronizerParameters(synchronizerAlias, sequencerAlias, client).value,
          NoExceptionRetryPolicy,
        )
    )
  }

  private def getBootstrapInfoSynchronizerParameters(
      synchronizerAlias: SynchronizerAlias
  )(connection: SequencerConnection)(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[
    FutureUnlessShutdown,
    SequencerInfoLoaderError,
    (SynchronizerClientBootstrapInfo, StaticSynchronizerParameters),
  ] =
    connection match {
      case grpc: GrpcSequencerConnection =>
        val client = sequencerConnectClientBuilder(synchronizerAlias, grpc)
        // retry the bootstrapping info parameters. we want to maximise the number of
        // sequencers (and work around a bootstrapping issue with active-active sequencers)
        // where k8 health end-points don't distinguish between admin / public api
        // and might route our requests to an active instance that is still waiting for
        // the node-id to be written to the database
        getBootstrapInfoSynchronizerParametersWithRetry(
          synchronizerAlias,
          grpc.sequencerAlias,
          client,
        ).thereafter(_ => client.close())
    }

  private def performHandshake(
      sequencerConnectClient: SequencerConnectClient,
      alias: SynchronizerAlias,
      sequencerAlias: SequencerAlias,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerInfoLoaderError, Unit] =
    for {
      success <- sequencerConnectClient
        .handshake(
          alias,
          HandshakeRequest(
            clientProtocolVersions,
            minimumProtocolVersion,
          ),
          dontWarnOnDeprecatedPV,
        )
        .leftMap(SequencerInfoLoader.fromSequencerConnectClientError(alias))
        .subflatMap {
          case success: HandshakeResponse.Success => success.asRight
          case HandshakeResponse.Failure(_, reason) =>
            SequencerInfoLoaderError.HandshakeFailedError(reason).asLeft
        }
    } yield {
      logger.info(
        s"Version handshake with sequencer $sequencerAlias and synchronizer using protocol version ${success.serverProtocolVersion} succeeded."
      )
      ()
    }

  def loadAndAggregateSequencerEndpoints(
      synchronizerAlias: SynchronizerAlias,
      expectedSynchronizerId: Option[SynchronizerId],
      sequencerConnections: SequencerConnections,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, SequencerInfoLoaderError, SequencerAggregatedInfo] = EitherT(
    loadSequencerEndpoints(
      synchronizerAlias,
      sequencerConnections,
      sequencerConnectionValidation == SequencerConnectionValidation.All,
    ).map(
      SequencerInfoLoader.aggregateBootstrapInfo(
        logger,
        sequencerTrustThreshold = sequencerConnections.sequencerTrustThreshold,
        submissionRequestAmplification = sequencerConnections.submissionRequestAmplification,
        sequencerConnectionValidation = sequencerConnectionValidation,
        expectedSynchronizerId = expectedSynchronizerId,
      )
    )
  )

  def validateSequencerConnection(
      alias: SynchronizerAlias,
      expectedSynchronizerId: Option[SynchronizerId],
      sequencerConnections: SequencerConnections,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, Seq[LoadSequencerEndpointInformationResult.NotValid], Unit] =
    sequencerConnectionValidation match {
      case SequencerConnectionValidation.Disabled => EitherT.rightT(())
      case SequencerConnectionValidation.All | SequencerConnectionValidation.Active =>
        EitherT(
          loadSequencerEndpoints(
            alias,
            sequencerConnections,
            sequencerConnectionValidation == SequencerConnectionValidation.All,
          )
            .map(
              SequencerInfoLoader.validateNewSequencerConnectionResults(
                expectedSynchronizerId,
                sequencerConnectionValidation,
                logger,
              )(_)
            )
        )
    }

  private def loadSequencerEndpoints(
      synchronizerAlias: SynchronizerAlias,
      sequencerConnections: SequencerConnections,
      loadAllEndpoints: Boolean,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): FutureUnlessShutdown[Seq[LoadSequencerEndpointInformationResult]] = {
    // if we want to validate all endpoints, we can expand the list of connections on a per-endpoint basis
    // during aggregation, we'll boil this down again
    val connections = if (loadAllEndpoints) {
      sequencerConnections.connections.flatMap { case connection: GrpcSequencerConnection =>
        connection.endpoints.map(endpoint =>
          connection.copy(endpoints = NonEmpty.mk(Seq, endpoint))
        )
      }
    } else
      sequencerConnections.connections

    loadSequencerEndpointsParallel(
      synchronizerAlias,
      connections,
      sequencerInfoLoadParallelism,
      Option.when(!loadAllEndpoints)(
        // TODO(#19911): Make the threshold configurable
        sequencerConnections.sequencerTrustThreshold * PositiveInt.two + PositiveInt.one
      ),
    ) { connection =>
      getBootstrapInfoSynchronizerParameters(synchronizerAlias)(connection).value
        .map {
          case Right((synchronizerClientBootstrapInfo, staticSynchronizerParameters)) =>
            LoadSequencerEndpointInformationResult.Valid(
              connection,
              synchronizerClientBootstrapInfo,
              staticSynchronizerParameters,
            )
          case Left(error) =>
            LoadSequencerEndpointInformationResult.NotValid(connection, error)
        }
    }
  }

  /** Load sequencer endpoints in parallel up to the specified parallelism.
    * If maybeThreshold is specified, potentially returns early if sufficiently many
    * valid endpoints are encountered. Also returns early if a failed future is
    * encountered.
    */
  @VisibleForTesting
  private[grpc] def loadSequencerEndpointsParallel(
      synchronizerAlias: SynchronizerAlias,
      connections: NonEmpty[Seq[SequencerConnection]],
      parallelism: NonNegativeInt,
      maybeThreshold: Option[PositiveInt],
  )(
      getInfo: SequencerConnection => FutureUnlessShutdown[LoadSequencerEndpointInformationResult]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[LoadSequencerEndpointInformationResult]] = {
    if (logger.underlying.isDebugEnabled()) {
      val grpcConnections = connections.collect { case grpc: GrpcSequencerConnection => grpc }
      logger.debug(
        s"Loading sequencer info entries with ${connections.size} connections (${grpcConnections
            .map(conn => s"${conn.sequencerAlias.unwrap}=${conn.endpoints.mkString(",")}")
            .mkString(";")}), parallelism ${parallelism.unwrap}, threshold $maybeThreshold, synchronizer ${synchronizerAlias.unwrap}"
      )
    }
    val promise = Promise[UnlessShutdown[Seq[LoadSequencerEndpointInformationResult]]]()
    val connectionsSize = connections.size
    val (initialConnections, remainingConnections) = connections.splitAt(parallelism.unwrap)
    val remainingAndDone =
      new AtomicReference[
        (
            Seq[SequencerConnection], // remaining sequencer connections
            Seq[LoadSequencerEndpointInformationResult], // results collected thus far
            Option[SequencerConnection], // next sequencer connection to load; dummy field used to move side-effecting code outside atomic reference
        )
      ](
        (remainingConnections, Seq.empty, None)
      )

    @nowarn("msg=match may not be exhaustive")
    def loadSequencerInfoAsync(connection: SequencerConnection): Unit = {
      logger.debug(
        s"About to load sequencer ${connection.sequencerAlias} info in synchronizer $synchronizerAlias"
      )
      // Note that we tested using HasFlushFuture.addToFlush, but the complexity and risk of delaying shutdown
      // wasn't worth the questionable benefit of tracking "dangling threads" without ownership of the netty channel.
      FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
        getInfo(connection).transform(
          {
            case Outcome(res) =>
              // Update atomic state using non-side-effecting code
              remainingAndDone.updateAndGet {
                case (Nil, resultsSoFar, _) => (Nil, res +: resultsSoFar, None)
                case (next +: rest, resultsSoFar, _) =>
                  (rest, res +: resultsSoFar, Some(next))
              } match {
                // Perform accounting to decide how to proceed.
                case (_, resultsSoFar, maybeNext) =>
                  logger.debug(
                    s"Loaded sequencer ${connection.sequencerAlias} info in synchronizer $synchronizerAlias"
                  )
                  if (!promise.isCompleted) {
                    if (
                      // done if all the results are in
                      resultsSoFar.sizeCompare(connectionsSize) >= 0 ||
                      // or if the threshold is reached with valid results
                      maybeThreshold.exists(threshold =>
                        resultsSoFar
                          .count {
                            case _: LoadSequencerEndpointInformationResult.Valid => true
                            case _: LoadSequencerEndpointInformationResult.NotValid => false
                          } >= threshold.unwrap
                      )
                    ) {
                      logger.debug(
                        s"Loaded sufficiently many sequencer info entries (${resultsSoFar.size}) in synchronizer $synchronizerAlias"
                      )
                      promise.trySuccess(Outcome(resultsSoFar.reverse)).discard
                    } else {
                      // otherwise load the next sequencer info if available
                      maybeNext.foreach(loadSequencerInfoAsync)
                    }
                  }
                  Outcome(())
              }
            case AbortedDueToShutdown =>
              promise.trySuccess(AbortedDueToShutdown).discard
              AbortedDueToShutdown
          },
          { t =>
            if (!promise.isCompleted) {
              LoggerUtil.logThrowableAtLevel(
                Level.ERROR,
                s"Exception loading sequencer ${connection.sequencerAlias} info in synchronizer $synchronizerAlias",
                t,
              )
              promise.tryFailure(t).discard
            } else {
              // Minimize log noise distraction on behalf of "dangling" futures.
              logger.info(
                s"Ignoring exception loading sequencer ${connection.sequencerAlias} info in synchronizer $synchronizerAlias after promise completion ${t.getMessage}"
              )
            }
            t
          },
        ),
        failureMessage =
          s"error on load sequencer ${connection.sequencerAlias} info in synchronizer $synchronizerAlias",
        level = if (promise.isCompleted) Level.INFO else Level.ERROR,
      )
    }

    initialConnections.foreach(loadSequencerInfoAsync)

    FutureUnlessShutdown(promise.future)
  }
}

object SequencerInfoLoader {

  sealed trait LoadSequencerEndpointInformationResult extends Product with Serializable

  object LoadSequencerEndpointInformationResult {
    final case class Valid(
        connection: SequencerConnection,
        synchronizerClientBootstrapInfo: SynchronizerClientBootstrapInfo,
        staticSynchronizerParameters: StaticSynchronizerParameters,
    ) extends LoadSequencerEndpointInformationResult
    final case class NotValid(
        sequencerConnection: SequencerConnection,
        error: SequencerInfoLoaderError,
    ) extends LoadSequencerEndpointInformationResult
  }

  final case class SequencerAggregatedInfo(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
      sequencerConnections: SequencerConnections,
  )

  sealed trait SequencerInfoLoaderError extends Product with Serializable {
    def cause: String
  }
  object SequencerInfoLoaderError {
    final case class DeserializationFailure(cause: String) extends SequencerInfoLoaderError
    final case class InvalidResponse(cause: String) extends SequencerInfoLoaderError
    final case class InvalidState(cause: String) extends SequencerInfoLoaderError
    final case class HandshakeFailedError(cause: String) extends SequencerInfoLoaderError
    final case class SequencersFromDifferentSynchronizersAreConfigured(cause: String)
        extends SequencerInfoLoaderError
    final case class InconsistentConnectivity(cause: String) extends SequencerInfoLoaderError
    final case class MisconfiguredStaticSynchronizerParameters(cause: String)
        extends SequencerInfoLoaderError
    final case class FailedToConnectToSequencers(cause: String) extends SequencerInfoLoaderError
    final case class SynchronizerIsNotAvailableError(alias: SynchronizerAlias, cause: String)
        extends SequencerInfoLoaderError
  }

  private def fromSequencerConnectClientError(alias: SynchronizerAlias)(
      error: SequencerConnectClient.Error
  ): SequencerInfoLoaderError = error match {
    case SequencerConnectClient.Error.DeserializationFailure(e) =>
      SequencerInfoLoaderError.DeserializationFailure(e)
    case SequencerConnectClient.Error.InvalidResponse(cause) =>
      SequencerInfoLoaderError.InvalidResponse(cause)
    case SequencerConnectClient.Error.InvalidState(cause) =>
      SequencerInfoLoaderError.InvalidState(cause)
    case SequencerConnectClient.Error.Transport(message) =>
      SequencerInfoLoaderError.SynchronizerIsNotAvailableError(alias, message)
  }

  /** Small utility function used to validate the sequencer connections whenever the configuration changes */
  def validateNewSequencerConnectionResults(
      expectedSynchronizerId: Option[SynchronizerId],
      sequencerConnectionValidation: SequencerConnectionValidation,
      logger: TracedLogger,
  )(
      results: Seq[LoadSequencerEndpointInformationResult]
  )(implicit
      traceContext: TraceContext
  ): Either[Seq[LoadSequencerEndpointInformationResult.NotValid], Unit] = {
    // now, check what failed and whether the reported sequencer ids and synchronizer ids aligned
    @tailrec
    def go(
        reference: Option[LoadSequencerEndpointInformationResult.Valid],
        sequencerIds: Map[SequencerId, SequencerAlias],
        rest: List[LoadSequencerEndpointInformationResult],
        accumulated: Seq[LoadSequencerEndpointInformationResult.NotValid],
    ): Seq[LoadSequencerEndpointInformationResult.NotValid] = rest match {
      case Nil =>
        accumulated
      case (notValid: LoadSequencerEndpointInformationResult.NotValid) :: rest =>
        if (sequencerConnectionValidation != SequencerConnectionValidation.All) {
          logger.info(
            s"Skipping validation, as I am unable to obtain synchronizer id and sequencer-id: ${notValid.error} for ${notValid.sequencerConnection}"
          )
          go(reference, sequencerIds, rest, accumulated)
        } else
          go(reference, sequencerIds, rest, notValid +: accumulated)
      case (valid: LoadSequencerEndpointInformationResult.Valid) :: rest =>
        val result = for {
          // check that synchronizer id matches the reference
          _ <- Either.cond(
            reference.forall(x =>
              x.synchronizerClientBootstrapInfo.synchronizerId == valid.synchronizerClientBootstrapInfo.synchronizerId
            ),
            (),
            SequencerInfoLoaderError.SequencersFromDifferentSynchronizersAreConfigured(
              show"Synchronizer id mismatch ${valid.synchronizerClientBootstrapInfo.synchronizerId} vs the first one found ${reference
                  .map(_.synchronizerClientBootstrapInfo.synchronizerId)}"
            ),
          )
          // check that static synchronizer parameters match
          _ <- Either.cond(
            reference.forall(x =>
              x.staticSynchronizerParameters == valid.staticSynchronizerParameters
            ),
            (),
            SequencerInfoLoaderError.MisconfiguredStaticSynchronizerParameters(
              show"Static synchronizer parameters mismatch ${valid.staticSynchronizerParameters.toString} vs the first one found ${reference
                  .map(_.staticSynchronizerParameters.toString)}"
            ),
          )
          // check that synchronizer id matches expected
          _ <- Either.cond(
            expectedSynchronizerId.forall(
              _ == valid.synchronizerClientBootstrapInfo.synchronizerId
            ),
            (),
            SequencerInfoLoaderError.InconsistentConnectivity(
              show"Synchronizer id ${valid.synchronizerClientBootstrapInfo.synchronizerId} does not match expected $expectedSynchronizerId"
            ),
          )
          // check that we don't have the same sequencer-id reported by different aliases
          _ <- Either.cond(
            sequencerIds
              .get(valid.synchronizerClientBootstrapInfo.sequencerId)
              .forall(_ == valid.connection.sequencerAlias),
            (),
            SequencerInfoLoaderError.InconsistentConnectivity(
              show"the same sequencer-id reported by different alias ${sequencerIds
                  .get(valid.synchronizerClientBootstrapInfo.sequencerId)}"
            ),
          )
          _ <- sequencerIds
            .collectFirst {
              case (sequencerId, alias)
                  if alias == valid.connection.sequencerAlias && sequencerId != valid.synchronizerClientBootstrapInfo.sequencerId =>
                SequencerInfoLoaderError.InconsistentConnectivity(
                  show"sequencer-id mismatch ${valid.synchronizerClientBootstrapInfo.sequencerId} vs previously observed $sequencerId"
                )
            }
            .toLeft(())
        } yield ()
        result match {
          case Right(_) =>
            go(
              Some(valid),
              sequencerIds.updated(
                valid.synchronizerClientBootstrapInfo.sequencerId,
                valid.connection.sequencerAlias,
              ),
              rest,
              accumulated,
            )
          case Left(error) =>
            go(
              reference,
              sequencerIds,
              rest,
              LoadSequencerEndpointInformationResult.NotValid(
                valid.connection,
                error,
              ) +: accumulated,
            )
        }

    }
    val collected = go(None, Map.empty, results.toList, Seq.empty)
    Either.cond(collected.isEmpty, (), collected)
  }

  /** Aggregates the endpoint information into the actual connection
    *
    * Given a set of sequencer connections and attempts to get the sequencer-id and synchronizer id
    * from each of them, we'll recompute the actual connections to be used.
    * Note that this method here would require a bit more smartness as whether a sequencer
    * is considered or not depends on whether it was up when we made the connection.
    */
  @VisibleForTesting
  private[grpc] def aggregateBootstrapInfo(
      logger: TracedLogger,
      sequencerTrustThreshold: PositiveInt,
      submissionRequestAmplification: SubmissionRequestAmplification,
      sequencerConnectionValidation: SequencerConnectionValidation,
      expectedSynchronizerId: Option[SynchronizerId],
  )(
      fullResult: Seq[LoadSequencerEndpointInformationResult]
  )(implicit
      traceContext: TraceContext
  ): Either[SequencerInfoLoaderError, SequencerAggregatedInfo] = {

    require(fullResult.nonEmpty, "Non-empty list of sequencerId-to-endpoint pair is expected")

    validateNewSequencerConnectionResults(
      expectedSynchronizerId,
      sequencerConnectionValidation,
      logger,
    )(
      fullResult.toList
    ) match {
      case Right(()) =>
        val validSequencerConnections = fullResult
          .collect { case valid: LoadSequencerEndpointInformationResult.Valid =>
            valid
          }
          .groupBy(_.connection.sequencerAlias)
          .flatMap { case (_, v) => v.headOption }
          .toSeq
        if (validSequencerConnections.sizeIs >= sequencerTrustThreshold.unwrap) {
          val validSequencerConnectionsNE = NonEmptyUtil.fromUnsafe(validSequencerConnections)
          val expectedSequencers = NonEmptyUtil.fromUnsafe(
            validSequencerConnectionsNE
              .groupBy(_.connection.sequencerAlias)
              .view
              .mapValues(_.map(_.synchronizerClientBootstrapInfo.sequencerId).head1)
              .toMap
          )
          SequencerConnections
            .many(
              validSequencerConnectionsNE.map(_.connection),
              sequencerTrustThreshold,
              submissionRequestAmplification,
            )
            .leftMap(SequencerInfoLoaderError.FailedToConnectToSequencers.apply)
            .map(connections =>
              SequencerAggregatedInfo(
                synchronizerId =
                  validSequencerConnectionsNE.head1.synchronizerClientBootstrapInfo.synchronizerId,
                staticSynchronizerParameters =
                  validSequencerConnectionsNE.head1.staticSynchronizerParameters,
                expectedSequencers = expectedSequencers,
                sequencerConnections = connections,
              )
            )
        } else {
          if (sequencerTrustThreshold.unwrap > 1)
            logger.warn(
              s"Insufficient valid sequencer connections ${validSequencerConnections.size} to reach threshold ${sequencerTrustThreshold.unwrap}"
            )
          val invalidSequencerConnections = fullResult.collect {
            case nonValid: LoadSequencerEndpointInformationResult.NotValid => nonValid
          }
          extractSingleError(invalidSequencerConnections).asLeft
        }
      case Left(value) => extractSingleError(value).asLeft
    }
  }

  private def extractSingleError(
      errors: Seq[LoadSequencerEndpointInformationResult.NotValid]
  ): SequencerInfoLoaderError = {
    require(errors.nonEmpty, "Non-empty list of errors is expected")
    val nonEmptyResult = NonEmptyUtil.fromUnsafe(errors)
    if (nonEmptyResult.sizeIs == 1) nonEmptyResult.head1.error
    else {
      val message = nonEmptyResult.map(_.error.cause).mkString(",")
      SequencerInfoLoaderError.FailedToConnectToSequencers(message)
    }
  }
}
