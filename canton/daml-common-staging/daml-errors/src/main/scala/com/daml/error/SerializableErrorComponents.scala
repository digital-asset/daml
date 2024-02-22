// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.NonSecuritySensitiveErrorCodeComponents.{stringsPackedSize, truncateDetails}
import com.daml.error.SerializableErrorCodeComponents.*
import com.daml.error.utils.ErrorDetails
import io.grpc.Status.Code

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.util.matching.Regex

object SerializableErrorCodeComponents {
  private[error] val ValidMetadataKeyRegex: Regex = "[^(a-zA-Z0-9-_)]".r
  private[error] val GrpcCodeBytes = 1
  // Each string is encoded additionally with a tag and a size value
  // Hence, we add some overhead to account for them
  private[error] val MaximumPerTagOverheadBytes = 5
  private[error] val ErrorInfoDetailPackingOverheadBytes =
    ErrorDetails.ErrorInfoDetail("", Map.empty).toRpcAny.getSerializedSize
  private[error] val ResourceInfoDetailPackingOverheadBytes =
    ErrorDetails.ResourceInfoDetail("", "").toRpcAny.getSerializedSize
  // By default, hex encoded correlation-ids and trace-ids have 32 characters
  // Since we don't have any explicit enforcement, truncate everything above 256 to disallow error serialization issues
  private[error] val MaxTraceIdCorrelationIdSize = 256

  def apply(
      errorCode: ErrorCode,
      loggingContext: ContextualizedErrorLogger,
      rawTraceId: Option[String],
      rawCorrelationId: Option[String],
      // Next parameters are by-name to avoid unnecessary computation if the error is security sensitive
      cause: => String,
      definiteAnswer: => Option[Boolean],
      errorResources: => Seq[(ErrorResource, String)],
      contextMap: => Map[String, String],
      retryableInfo: => Option[FiniteDuration],
  ): SerializableErrorCodeComponents = {
    val (traceId, correlationId) =
      validateTraceIdAndCorrelationId(rawTraceId, rawCorrelationId)(loggingContext)

    if (errorCode.category.securitySensitive)
      SecuritySensitiveErrorCodeComponents(
        grpcStatusCode = errorCode.category.grpcCode,
        traceId = traceId,
        correlationId = correlationId,
      )(logger = loggingContext)
    else
      NonSecuritySensitiveErrorCodeComponents(
        traceId = traceId,
        correlationId = correlationId,
        cause = cause,
        errorCode = errorCode,
        definiteAnswer = definiteAnswer,
        errorResources = errorResources,
        contextMap = contextMap ++ loggingContext.properties,
        retryableInfo = retryableInfo,
      )(loggingContext = loggingContext)
  }

  private[error] def validateTraceIdAndCorrelationId(
      rawTraceId: Option[String],
      rawCorrelationId: Option[String],
  )(implicit loggingContext: ContextualizedErrorLogger) = {
    val traceId = rawTraceId.map(tId =>
      truncateString(
        tId,
        MaxTraceIdCorrelationIdSize,
        loggingContext.warn(
          s"Trace-id $tId exceeded maximum allowed size of $MaxTraceIdCorrelationIdSize and has been truncated for gRPC error serialization"
        ),
      )
    )
    val correlationId = rawCorrelationId.map(cId =>
      truncateString(
        cId,
        MaxTraceIdCorrelationIdSize,
        loggingContext.warn(
          s"Correlation-id $cId exceeded maximum allowed size of $MaxTraceIdCorrelationIdSize and has been truncated for gRPC error serialization"
        ),
      )
    )
    (traceId, correlationId)
  }

  private[error] def truncateString(v: String, maxSize: Int, onTruncate: => Unit = ()): String =
    if (v.length > maxSize) {
      onTruncate
      s"${v.take(maxSize - 3)}..."
    } else v
}

sealed trait SerializableErrorCodeComponents {
  def toStatusProto(maxSizeBytes: Int): com.google.rpc.Status
}

private[error] final case class SecuritySensitiveErrorCodeComponents(
    grpcStatusCode: Option[Code],
    traceId: Option[String],
    correlationId: Option[String],
)(logger: ContextualizedErrorLogger)
    extends SerializableErrorCodeComponents {

  override def toStatusProto(maxSizeBytes: Int): com.google.rpc.Status =
    com.google.rpc.Status
      .newBuilder()
      .setCode(
        grpcStatusCode
          .getOrElse {
            logger.warn("Missing grpc status code for security sensitive error")
            Code.INTERNAL
          }
          .value()
      )
      .setMessage(BaseError.SecuritySensitiveMessage(correlationId, traceId))
      .addAllDetails(
        correlationId
          .orElse(traceId)
          .map(ErrorDetails.RequestInfoDetail)
          .toList
          .map(_.toRpcAny)
          .asJava
      )
      .build()
}

private[error] final case class NonSecuritySensitiveErrorCodeComponents(
    traceId: Option[String],
    correlationId: Option[String],
    cause: String,
    errorCode: ErrorCode,
    definiteAnswer: Option[Boolean],
    errorResources: Seq[(ErrorResource, String)],
    contextMap: Map[String, String],
    retryableInfo: Option[FiniteDuration],
)(loggingContext: ContextualizedErrorLogger)
    extends SerializableErrorCodeComponents {

  /** Truncates and serializes the self-service error components into a [[com.google.rpc.Status]].
    *
    * Truncation happens for both the error message and error details aiming to ensure that
    * the maximum message size ([[ErrorCode.MaxCauseLogLength]]) and maximum total Status serialization size ([[ErrorCode.MaxErrorContentBytes]]) are respected.
    */
  def toStatusProto(maxSizeBytes: Int): com.google.rpc.Status = {
    val grpcStatusCode = validatedGrpcErrorCode(errorCode.category.grpcCode)
    val errorCategoryContext = "category" -> errorCode.category.asInt.toString
    val traceIdContext = traceId.map("tid" -> _)
    val definiteAnswerContext =
      definiteAnswer.map(value => GrpcStatuses.DefiniteAnswerKey -> value.toString)

    val retryInfoRpc = retryableInfo.map(ErrorDetails.RetryInfoDetail(_).toRpcAny)
    val requestInfoRpc =
      correlationId.orElse(traceId).map(ErrorDetails.RequestInfoDetail(_).toRpcAny)
    val errorCodeId = errorCode.id

    val validatedMessage = errorCode.toMsg(cause, correlationId)
    val mandatoryDetailsEncodedSize =
      GrpcCodeBytes + MaximumPerTagOverheadBytes + // Grpc code size + its byte tag overhead
        stringsPackedSize(
          Seq(validatedMessage, errorCodeId, errorCategoryContext._1, errorCategoryContext._2) ++
            traceIdContext.map(v => Seq(v._1, v._2)).getOrElse(Seq.empty) ++
            definiteAnswerContext.map(v => Seq(v._1, v._2)).getOrElse(Seq.empty)
        ) +
        retryInfoRpc.fold(0)(_.getSerializedSize + MaximumPerTagOverheadBytes) +
        requestInfoRpc.fold(0)(_.getSerializedSize + MaximumPerTagOverheadBytes) +
        // overhead bytes for packing ErrorInfo
        ErrorInfoDetailPackingOverheadBytes

    val bytesLeftForTruncateableDetails = maxSizeBytes - mandatoryDetailsEncodedSize

    // Truncate-able error details
    val (truncatedContext, truncatedErrorResources) =
      truncateDetails(contextMap, errorResources, bytesLeftForTruncateableDetails)

    val errorInfoDetail = ErrorDetails.ErrorInfoDetail(
      errorCodeId = errorCodeId,
      // For simplicity, all key-values added here have been accounted with tag overheads
      // even though they are not tagged when serialized.
      metadata =
        (truncatedContext ++ definiteAnswerContext.toList ++ traceIdContext.toList :+ errorCategoryContext).toMap,
    )

    val resourceInfos =
      truncatedErrorResources.view.map(_.swap).map(ErrorDetails.ResourceInfoDetail.tupled).toList

    val allDetails =
      Seq(errorInfoDetail.toRpcAny) ++ retryInfoRpc.toList ++ requestInfoRpc.toList ++ resourceInfos
        .map(_.toRpcAny)

    // Build status
    com.google.rpc.Status
      .newBuilder()
      .setCode(grpcStatusCode.value())
      .setMessage(validatedMessage)
      .addAllDetails(allDetails.asJava)
      .build()
  }

  private def validatedGrpcErrorCode(grpcCode: Option[Code]): Code =
    grpcCode.getOrElse {
      loggingContext.warn(s"Passing non-grpc error via grpc ${errorCode.id} ")
      Code.INTERNAL
    }
}

private[error] object NonSecuritySensitiveErrorCodeComponents {
  private[error] def truncateDetails(
      context: Map[String, String],
      errResources: Seq[(ErrorResource, String)],
      remainingBudgetBytes: Int,
  ): (Seq[(String, String)], Seq[(String, String)]) = {

    val numberOfEntries = context.size + errResources.size
    if (numberOfEntries == 0) (Seq.empty, Seq.empty)
    else {
      val remainingBudgetForEntries = remainingBudgetBytes -
        // account for two tags per key-value pair
        2 * MaximumPerTagOverheadBytes * numberOfEntries

      val budgetForTruncatedResources =
        (errResources.size.toLong * remainingBudgetForEntries.toLong) / numberOfEntries.toLong

      val budgetForTruncatedContext = remainingBudgetForEntries - budgetForTruncatedResources

      val truncatedErrorResources = truncateResources(
        errResources.map { case (res, v) => res.asString -> v },
        budgetForTruncatedResources.toInt,
      )

      val truncatedContext = truncateContext(context.toSeq, budgetForTruncatedContext.toInt)
      (truncatedContext, truncatedErrorResources)
    }
  }

  private[error] def truncateContext(
      rawContextEntries: Seq[(String, String)],
      maxBudgetBytes: Int,
  ): Seq[(String, String)] = {
    val raw: Seq[(String, String)] = rawContextEntries.view
      // Make key gRPC compliant
      .map { case (k, v) => ValidMetadataKeyRegex.replaceAllIn(k, "").take(63) -> v }
      // Discard empty values
      .filter(_._1.nonEmpty)
      // Discard empty keys
      .filter(_._2.nonEmpty)
      .toSeq
      .sortBy(v => v._1.length + v._2.length)

    val rawSize = raw.size

    raw.view.zipWithIndex
      .foldLeft((Vector.empty[(String, String)], maxBudgetBytes)) {
        case ((acc, free), ((k, v), idx)) =>
          // This value can be regarded as an "inverse" moving average,
          // (i.e. computed on the remaining entries instead of the past ones).
          // Since the series is sorted by the entries size, this value progresses strictly ascending
          // which allows more aggressive truncation of the bigger/later entries of this series.
          val maxSize = free / (rawSize - idx)

          val maybeNewEntry = if (k.length + v.length > maxSize) {
            // We need at least 8 chars per (k,v) entry to get something meaningful after truncation
            // since we might suffix each of the k,v strings with `...` (3 chars) - see truncateString
            Option.when(maxSize > 7) {
              val truncatedKey = truncateString(k, maxSize / 2)
              truncatedKey -> truncateString(v, maxSize - truncatedKey.length)
            }
          } else Some(k -> v)

          maybeNewEntry
            .map { case (k, v) => (k, v, encSize(k) + encSize(v)) }
            // Check again that unexpected encoding did not lead to exceeding the budget
            // (For simplicity, in truncateString, we assume 1-byte per string encoding)
            .filter { case (_, _, encodedSize) => encodedSize <= maxSize }
            .map { case (newK, newV, encodedSize) => (acc :+ (newK -> newV), free - encodedSize) }
            .getOrElse((acc, free))
      }
      ._1
  }

  private def truncateResources(
      details: Seq[(String, String)],
      remaining: Int,
  ): Seq[(String, String)] =
    details
      .foldLeft(Vector.empty[(String, String)] -> remaining) { case ((acc, free), (k, v)) =>
        // Account for the resource being packed as a ResourceInfo
        val newFree =
          free - (encSize(k) + encSize(v) + ResourceInfoDetailPackingOverheadBytes)
        if (newFree < 0) (acc, free)
        else (acc :+ (k -> v), newFree)
      }
      ._1

  private def stringsPackedSize(strings: Seq[String]): Int =
    strings.map(encSize(_) + MaximumPerTagOverheadBytes).sum

  private def encSize(s: String): Int = s.getBytes("UTF-8").length
}
