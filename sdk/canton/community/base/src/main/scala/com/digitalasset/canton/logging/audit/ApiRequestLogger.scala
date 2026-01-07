// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.audit

import com.digitalasset.canton.auth.ClaimSet
import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CallMetadata
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

import java.net.InetSocketAddress

/** Encloses logic for logging incoming requests and their outcomes.
  *
  * Enable debug for this class to see incoming requests and their outcomes. example logback
  * configuration: <logger name="com.digitalasset.canton.logging.audit" level="DEBUG"/>
  *
  * @see
  *   ApiLoggingConfig for configuration options
  */
class ApiRequestLogger(
    config: ApiLoggingConfig,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private lazy val printer = config.printer

  def logIncomingRequest(
      call: CallMetadata,
      message: Any,
  )(implicit
      traceContext: TraceContext
  ): Unit =
    debugLog(
      call,
      createLogMessage(call, show"received a message ${cutMessage(message).unquoted}"),
    )

  def logAuth(callMetadata: CallMetadata, claimSet: ClaimSet)(implicit
      traceContext: TraceContext
  ): Unit =
    debugLog(callMetadata, createLogMessage(callMetadata, s"auth claims: $claimSet"))

  def logAuthError(callMetadata: CallMetadata, error: Throwable)(implicit
      traceContext: TraceContext
  ): Unit =
    debugLog(callMetadata, createLogMessage(callMetadata, error.getMessage))

  def logResponseStatus(
      call: CallMetadata,
      responseKind: ResponseKind,
      status: String,
      errorCause: Option[Throwable],
  )(implicit traceContext: TraceContext) = errorCause match {
    case Some(cause) =>
      responseKind match {
        case ResponseKind.SevereError => logger.error(createLogMessage(call, s"$status"), cause)
        // TODO (i28285) - change to warn
        case ResponseKind.Security => logger.debug(createLogMessage(call, s"$status"), cause)
        case ResponseKind.MinorError => logger.info(createLogMessage(call, s"$status"), cause)
        case ResponseKind.OK =>
          logger.debug(createLogMessage(call, s"$status"), cause)
      }

    case None =>
      responseKind match {
        // TODO (i28285) - change to warn
        case ResponseKind.Security => logger.debug(createLogMessage(call, s"$status"))
        case ResponseKind.SevereError => logger.error(createLogMessage(call, s"$status"))
        case ResponseKind.MinorError => logger.info(createLogMessage(call, s"$status"))
        case ResponseKind.OK => debugLog(call, createLogMessage(call, s"$status"))
      }

  }

  // Extra methods for logging ad hoc messages at various levels
  // provided for compatibility with existing code and logs
  def traceMessage(
      call: CallMetadata,
      message: String,
      content: Option[Any] = None,
  )(implicit
      traceContext: TraceContext
  ): Unit =
    content.fold(logger.trace(createLogMessage(call, s"$message"))) { details =>
      logger.trace(createLogMessage(call, s"$message ${cutMessage(details).unquoted}"))
    }

  def debugMessage(
      call: CallMetadata,
      message: String,
      content: Option[Any] = None,
  )(implicit
      traceContext: TraceContext
  ): Unit = content.fold(debugLog(call, createLogMessage(call, message))) { details =>
    debugLog(call, createLogMessage(call, s"$message ${cutMessage(details).unquoted}"))
  }

  private def debugLog(call: CallMetadata, message: String)(implicit
      traceContext: TraceContext
  ): Unit =
    call.transport match {
      case TransportType.InProcessGrpc if !config.debugInProcessRequests =>
        logger.trace(message)
      case _ =>
        logger.debug(message)
    }

  private def show(
      transportType: TransportType,
      clientAddr: Either[String, InetSocketAddress],
  ): String = {
    val addressPart = clientAddr match {
      case Left(name) => name
      case Right(addr) => addr.toString
    }
    val prefix = transportType match {
      case TransportType.Http => "http:"
      case TransportType.HttpWs => "ws:"
      case TransportType.Grpc | TransportType.InProcessGrpc if !config.prefixGrpcAddresses => ""
      case TransportType.Grpc => "grpc:"
      case TransportType.InProcessGrpc => "in-process-grpc:"
    }
    s"$prefix$addressPart"
  }

  private def createLogMessage(call: CallMetadata, message: String): String =
    s"Request ${call.apiEndpoint} by ${show(call.transport, call.remoteAddress)}: $message"

  @SuppressWarnings(Array("org.wartremover.warts.Product"))
  protected def cutMessage(message: Any): String =
    if (config.messagePayloads) {
      printer.printAdHoc(message)
    } else ""
}

sealed trait TransportType extends Product with Serializable

object TransportType {
  case object Grpc extends TransportType
  case object InProcessGrpc extends TransportType
  case object Http extends TransportType
  case object HttpWs extends TransportType
}

sealed trait ResponseKind {}

object ResponseKind {
  case object OK extends ResponseKind
  // Server side errors, unexpected exceptions etc.
  case object SevereError extends ResponseKind
  // Client related errors, communication related errors etc.
  case object MinorError extends ResponseKind
  case object Security extends ResponseKind
}
