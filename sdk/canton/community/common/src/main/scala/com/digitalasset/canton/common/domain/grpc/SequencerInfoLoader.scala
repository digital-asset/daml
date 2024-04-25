// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain.grpc

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.*
import com.digitalasset.canton.common.domain.SequencerConnectClient
import com.digitalasset.canton.common.domain.SequencerConnectClient.DomainClientBootstrapInfo
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader.{
  LoadSequencerEndpointInformationResult,
  SequencerAggregatedInfo,
  SequencerInfoLoaderError,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnectionValidation,
  SequencerConnections,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry.RetryUtil.NoExnRetryable
import com.digitalasset.canton.util.{MonadUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContextExecutor, Future}

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
    (config: SequencerConnection) =>
      SequencerConnectClient(
        config,
        timeouts,
        traceContextPropagation,
        loggerFactory,
      )
  }

  private def getBootstrapInfoDomainParameters(
      domainAlias: DomainAlias,
      sequencerAlias: SequencerAlias,
      client: SequencerConnectClient,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    Future,
    SequencerInfoLoaderError,
    (DomainClientBootstrapInfo, StaticDomainParameters),
  ] = {
    for {
      bootstrapInfo <- client
        .getDomainClientBootstrapInfo(domainAlias)
        .leftMap(SequencerInfoLoader.fromSequencerConnectClientError(domainAlias))

      _ <- performHandshake(client, domainAlias, sequencerAlias)

      domainParameters <- client
        .getDomainParameters(domainAlias.unwrap)
        .leftMap(SequencerInfoLoader.fromSequencerConnectClientError(domainAlias))
    } yield (bootstrapInfo, domainParameters)
  }

  private def getBootstrapInfoDomainParametersWithRetry(
      domainAlias: DomainAlias,
      sequencerAlias: SequencerAlias,
      client: SequencerConnectClient,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[
    Future,
    SequencerInfoLoaderError,
    (DomainClientBootstrapInfo, StaticDomainParameters),
  ] = {
    val retries = 10
    EitherT(
      retry
        .Pause(
          logger,
          performUnlessClosing = closeContext.context,
          maxRetries = retries,
          delay = timeouts.sequencerInfo.asFiniteApproximation.div(retries.toLong),
          operationName = functionFullName,
        )
        .apply(
          getBootstrapInfoDomainParameters(domainAlias, sequencerAlias, client).value,
          NoExnRetryable,
        )
    )
  }

  private def getBootstrapInfoDomainParameters(
      domainAlias: DomainAlias
  )(connection: SequencerConnection)(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[
    Future,
    SequencerInfoLoaderError,
    (DomainClientBootstrapInfo, StaticDomainParameters),
  ] =
    connection match {
      case grpc: GrpcSequencerConnection =>
        for {
          client <- sequencerConnectClientBuilder(grpc).leftMap(
            SequencerInfoLoader.fromSequencerConnectClientError(domainAlias)
          )
          // retry the bootstrapping info parameters. we want to maximise the number of
          // sequencers (and work around a bootstrapping issue with active-active sequencers)
          // where k8 health end-points don't distinguish between admin / public api
          // and might route our requests to an active instance that is still waiting for
          // the node-id to be written to the database
          bootstrapInfoDomainParameters <- getBootstrapInfoDomainParametersWithRetry(
            domainAlias,
            grpc.sequencerAlias,
            client,
          )
            .thereafter(_ => client.close())
        } yield bootstrapInfoDomainParameters
    }

  private def performHandshake(
      sequencerConnectClient: SequencerConnectClient,
      alias: DomainAlias,
      sequencerAlias: SequencerAlias,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerInfoLoaderError, Unit] =
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
        s"Version handshake with sequencer ${sequencerAlias} and domain using protocol version ${success.serverProtocolVersion} succeeded."
      )
      ()
    }

  def loadAndAggregateSequencerEndpoints(
      domainAlias: DomainAlias,
      sequencerConnections: SequencerConnections,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[Future, SequencerInfoLoaderError, SequencerAggregatedInfo] = EitherT(
    loadSequencerEndpoints(
      domainAlias,
      sequencerConnections,
      sequencerConnectionValidation == SequencerConnectionValidation.All,
    ).map(
      SequencerInfoLoader.aggregateBootstrapInfo(
        logger,
        sequencerTrustThreshold = sequencerConnections.sequencerTrustThreshold,
        submissionRequestAmplification = sequencerConnections.submissionRequestAmplification,
        sequencerConnectionValidation = sequencerConnectionValidation,
      )
    )
  )

  def validateSequencerConnection(
      alias: DomainAlias,
      expectedDomainId: Option[DomainId],
      sequencerConnections: SequencerConnections,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[Future, Seq[LoadSequencerEndpointInformationResult.NotValid], Unit] =
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
                expectedDomainId,
                sequencerConnectionValidation,
                logger,
              )(_)
            )
        )
    }

  private def loadSequencerEndpoints(
      domainAlias: DomainAlias,
      sequencerConnections: SequencerConnections,
      loadAllEndpoints: Boolean,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[Seq[LoadSequencerEndpointInformationResult]] = {
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
    MonadUtil
      .parTraverseWithLimit(
        parallelism = sequencerInfoLoadParallelism.unwrap
      )(connections) { connection =>
        getBootstrapInfoDomainParameters(domainAlias)(connection).value
          .map {
            case Right((domainClientBootstrapInfo, staticDomainParameters)) =>
              LoadSequencerEndpointInformationResult.Valid(
                connection,
                domainClientBootstrapInfo,
                staticDomainParameters,
              )
            case Left(error) =>
              LoadSequencerEndpointInformationResult.NotValid(connection, error)
          }
      }
  }

}

object SequencerInfoLoader {

  sealed trait LoadSequencerEndpointInformationResult extends Product with Serializable

  object LoadSequencerEndpointInformationResult {
    final case class Valid(
        connection: SequencerConnection,
        domainClientBootstrapInfo: DomainClientBootstrapInfo,
        staticDomainParameters: StaticDomainParameters,
    ) extends LoadSequencerEndpointInformationResult
    final case class NotValid(
        sequencerConnection: SequencerConnection,
        error: SequencerInfoLoaderError,
    ) extends LoadSequencerEndpointInformationResult
  }

  final case class SequencerAggregatedInfo(
      domainId: DomainId,
      staticDomainParameters: StaticDomainParameters,
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
    final case class SequencersFromDifferentDomainsAreConfigured(cause: String)
        extends SequencerInfoLoaderError
    final case class InconsistentConnectivity(cause: String) extends SequencerInfoLoaderError
    final case class MisconfiguredStaticDomainParameters(cause: String)
        extends SequencerInfoLoaderError
    final case class FailedToConnectToSequencers(cause: String) extends SequencerInfoLoaderError
    final case class DomainIsNotAvailableError(alias: DomainAlias, cause: String)
        extends SequencerInfoLoaderError
  }

  private def fromSequencerConnectClientError(alias: DomainAlias)(
      error: SequencerConnectClient.Error
  ): SequencerInfoLoaderError = error match {
    case SequencerConnectClient.Error.DeserializationFailure(e) =>
      SequencerInfoLoaderError.DeserializationFailure(e)
    case SequencerConnectClient.Error.InvalidResponse(cause) =>
      SequencerInfoLoaderError.InvalidResponse(cause)
    case SequencerConnectClient.Error.InvalidState(cause) =>
      SequencerInfoLoaderError.InvalidState(cause)
    case SequencerConnectClient.Error.Transport(message) =>
      SequencerInfoLoaderError.DomainIsNotAvailableError(alias, message)
  }

  /** Small utility function used to validate the sequencer connections whenever the configuration changes */
  def validateNewSequencerConnectionResults(
      expectedDomainId: Option[DomainId],
      sequencerConnectionValidation: SequencerConnectionValidation,
      logger: TracedLogger,
  )(
      results: Seq[LoadSequencerEndpointInformationResult]
  )(implicit
      traceContext: TraceContext
  ): Either[Seq[LoadSequencerEndpointInformationResult.NotValid], Unit] = {
    // now, check what failed and whether the reported sequencer ids and domain-ids aligned
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
            s"Skipping validation, as I am unable to obtain domain-id and sequencer-id: ${notValid.error} for ${notValid.sequencerConnection}"
          )
          go(reference, sequencerIds, rest, accumulated)
        } else
          go(reference, sequencerIds, rest, notValid +: accumulated)
      case (valid: LoadSequencerEndpointInformationResult.Valid) :: rest =>
        val result = for {
          // check that domain-id matches the reference
          _ <- Either.cond(
            reference.forall(x =>
              x.domainClientBootstrapInfo.domainId == valid.domainClientBootstrapInfo.domainId
            ),
            (),
            SequencerInfoLoaderError.SequencersFromDifferentDomainsAreConfigured(
              show"Domain-id mismatch ${valid.domainClientBootstrapInfo.domainId} vs the first one found ${reference
                  .map(_.domainClientBootstrapInfo.domainId)}"
            ),
          )
          // check that static domain parameters match
          _ <- Either.cond(
            reference.forall(x => x.staticDomainParameters == valid.staticDomainParameters),
            (),
            SequencerInfoLoaderError.MisconfiguredStaticDomainParameters(
              show"Static domain parameters mismatch ${valid.staticDomainParameters.toString} vs the first one found ${reference
                  .map(_.staticDomainParameters.toString)}"
            ),
          )
          // check that domain-id matches expected
          _ <- Either.cond(
            expectedDomainId.forall(_ == valid.domainClientBootstrapInfo.domainId),
            (),
            SequencerInfoLoaderError.InconsistentConnectivity(
              show"Domain-id ${valid.domainClientBootstrapInfo.domainId} does not match expected ${expectedDomainId}"
            ),
          )
          // check that we don't have the same sequencer-id reported by different aliases
          _ <- Either.cond(
            sequencerIds
              .get(valid.domainClientBootstrapInfo.sequencerId)
              .forall(_ == valid.connection.sequencerAlias),
            (),
            SequencerInfoLoaderError.InconsistentConnectivity(
              show"the same sequencer-id reported by different alias ${sequencerIds
                  .get(valid.domainClientBootstrapInfo.sequencerId)}"
            ),
          )
          _ <- sequencerIds
            .collectFirst {
              case (sequencerId, alias)
                  if alias == valid.connection.sequencerAlias && sequencerId != valid.domainClientBootstrapInfo.sequencerId =>
                SequencerInfoLoaderError.InconsistentConnectivity(
                  show"sequencer-id mismatch ${valid.domainClientBootstrapInfo.sequencerId} vs previously observed ${sequencerId}"
                )
            }
            .toLeft(())
        } yield ()
        result match {
          case Right(_) =>
            go(
              Some(valid),
              sequencerIds.updated(
                valid.domainClientBootstrapInfo.sequencerId,
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
    * Given a set of sequencer connections and attempts to get the sequencer-id and domain-id
    * from each of them, we'll recompute the actual connections to be used.
    * Note that this method here would require a bit more smartness as whether a sequencer
    * is considered or not depends on whether it was up when we made the connection.
    */
  @VisibleForTesting
  private[grpc] def aggregateBootstrapInfo(
      logger: TracedLogger,
      sequencerTrustThreshold: PositiveInt,
      submissionRequestAmplification: PositiveInt,
      sequencerConnectionValidation: SequencerConnectionValidation,
  )(
      fullResult: Seq[LoadSequencerEndpointInformationResult]
  )(implicit
      traceContext: TraceContext
  ): Either[SequencerInfoLoaderError, SequencerAggregatedInfo] = {

    require(fullResult.nonEmpty, "Non-empty list of sequencerId-to-endpoint pair is expected")

    validateNewSequencerConnectionResults(None, sequencerConnectionValidation, logger)(
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
          val nonEmptyResult = NonEmptyUtil.fromUnsafe(validSequencerConnections)
          val expectedSequencers = NonEmptyUtil.fromUnsafe(
            nonEmptyResult
              .groupBy(_.connection.sequencerAlias)
              .view
              .mapValues(_.map(_.domainClientBootstrapInfo.sequencerId).head1)
              .toMap
          )
          SequencerConnections
            .many(
              nonEmptyResult.map(_.connection),
              sequencerTrustThreshold,
              submissionRequestAmplification,
            )
            .leftMap(SequencerInfoLoaderError.FailedToConnectToSequencers)
            .map(connections =>
              SequencerAggregatedInfo(
                domainId = nonEmptyResult.head1.domainClientBootstrapInfo.domainId,
                staticDomainParameters = nonEmptyResult.head1.staticDomainParameters,
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
    if (nonEmptyResult.size == 1) nonEmptyResult.head1.error
    else {
      val message = nonEmptyResult.map(_.error.cause).mkString(",")
      SequencerInfoLoaderError.FailedToConnectToSequencers(message)
    }
  }

}
