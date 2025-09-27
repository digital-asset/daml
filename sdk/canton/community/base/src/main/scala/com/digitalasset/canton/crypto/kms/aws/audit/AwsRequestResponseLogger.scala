// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.aws.audit

import cats.Show
import com.digitalasset.canton.crypto.kms.audit.KmsRequestResponseLogger
import com.digitalasset.canton.crypto.kms.aws.audit.AwsRequestResponseLogger.{
  sdkRequestPretty,
  sdkResponsePretty,
}
import com.digitalasset.canton.crypto.kms.aws.tracing.AwsTraceContextInterceptor.{
  otelSpanExecutionAttribute,
  withTraceContext,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil
import software.amazon.awssdk.awscore.AwsResponse
import software.amazon.awssdk.core.interceptor.{
  Context,
  ExecutionAttribute,
  ExecutionAttributes,
  ExecutionInterceptor,
}
import software.amazon.awssdk.core.{SdkRequest, SdkResponse}
import software.amazon.awssdk.services.kms.model.*

import scala.compat.java8.OptionConverters.RichOptionalGeneric

/** AWS SDK execution interceptor that logs all requests and responses. Retrieves the canton trace
  * context via execution attributes.
  *
  * Logs every request before it's being sent, and every response after it's been deserialized,
  * along with the request that triggered it, at INFO level. Every failed request will also be
  * logged at WARN level, with the optional response if available.
  */
class AwsRequestResponseLogger(override val loggerFactory: NamedLoggerFactory)
    extends NamedLogging
    with ExecutionInterceptor
    with ShowUtil {

  private def getSpanId(executionAttributes: ExecutionAttributes): String =
    Option(executionAttributes.getAttribute(otelSpanExecutionAttribute))
      .map(_.getSpanContext.getSpanId)
      .getOrElse("no-id")

  override def beforeTransmission(
      context: Context.BeforeTransmission,
      executionAttributes: ExecutionAttributes,
  ): Unit = withTraceContext(executionAttributes, logger) { implicit tc =>
    logger.info(
      show"Sending request [${getSpanId(executionAttributes)}]: ${context
          .request()} to ${context.httpRequest().getUri.show}"
    )
  }

  override def afterUnmarshalling(
      context: Context.AfterUnmarshalling,
      executionAttributes: ExecutionAttributes,
  ): Unit = withTraceContext(executionAttributes, logger) { implicit tc =>
    logger.info(
      show"Received response [${getSpanId(executionAttributes)}]: ${context.response()}"
    )
  }

  override def onExecutionFailure(
      context: Context.FailedExecution,
      executionAttributes: ExecutionAttributes,
  ): Unit = withTraceContext(executionAttributes, logger) { implicit tc =>
    logger.warn(
      s"Request [${getSpanId(executionAttributes)}] failed.${context.response().asScala.map(r => s" Response: ${r.show}").getOrElse("")}",
      context.exception(),
    )
  }
}

object AwsRequestResponseLogger extends KmsRequestResponseLogger with ShowUtil {
  private[aws] val traceContextExecutionAttribute =
    new ExecutionAttribute[TraceContext]("canton-trace-context")

  private def awsRequestLog(response: AwsResponse) =
    s"[Aws-Id: ${response.responseMetadata().requestId()}]"

  private val createKeyRequestShow: Show[CreateKeyRequest] = { request =>
    createKeyRequestMsg(request.keyUsageAsString(), request.keySpecAsString())
  }

  private val createKeyResponseShow: Show[CreateKeyResponse] = { response =>
    s"${awsRequestLog(response)} - " +
      createKeyResponseMsg(
        response.keyMetadata().keyId(),
        response.keyMetadata().keyUsageAsString(),
        response.keyMetadata().keySpecAsString(),
      )
  }

  private val getPublicKeyRequestShow: Show[GetPublicKeyRequest] = { request =>
    getPublicKeyRequestMsg(request.keyId())
  }

  private val getPublicKeyResponseShow: Show[GetPublicKeyResponse] = { response =>
    s"${awsRequestLog(response)} - " +
      getPublicKeyResponseMsg(response.keyId(), response.keySpecAsString())
  }

  private val retrieveKeyMetadataRequestShow: Show[DescribeKeyRequest] = { request =>
    retrieveKeyMetadataRequestMsg(request.keyId())
  }

  private val retrieveKeyMetadataResponseShow: Show[DescribeKeyResponse] = { response =>
    s"${awsRequestLog(response)} - " +
      retrieveKeyMetadataResponseMsg(
        response.keyMetadata().keyId(),
        response.keyMetadata().keySpecAsString(),
        response.keyMetadata().keyStateAsString(),
      )
  }

  // Pretty instances for encrypt/decrypt request/response
  private val encryptRequestShow: Show[EncryptRequest] = { request =>
    encryptRequestMsg(request.keyId, request.encryptionAlgorithmAsString)
  }

  private val encryptResponseShow: Show[EncryptResponse] = { response =>
    s"${awsRequestLog(response)} - " +
      encryptResponseMsg(response.keyId, response.encryptionAlgorithmAsString)
  }

  private val decryptRequestShow: Show[DecryptRequest] = { request =>
    decryptRequestMsg(request.keyId, request.encryptionAlgorithmAsString)
  }

  private val decryptResponseShow: Show[DecryptResponse] = { response =>
    s"${awsRequestLog(response)} - " +
      decryptResponseMsg(response.keyId, response.encryptionAlgorithmAsString)
  }

  private val signRequestShow: Show[SignRequest] = { request =>
    signRequestMsg(request.keyId, request.messageTypeAsString, request.signingAlgorithmAsString)
  }

  private val signResponseShow: Show[SignResponse] = { response =>
    s"${awsRequestLog(response)} - " +
      signResponseMsg(response.keyId, response.signingAlgorithmAsString)
  }

  private val deleteKeyRequestShow: Show[ScheduleKeyDeletionRequest] = { request =>
    deleteKeyRequestMsg(request.keyId)
  }

  private val deleteKeyResponseShow: Show[ScheduleKeyDeletionResponse] = { response =>
    s"${awsRequestLog(response)} - " +
      deleteKeyResponseMsg(response.keyId)
  }

  implicit val sdkRequestPretty: Show[SdkRequest] = {
    case createKeyRequest: CreateKeyRequest => createKeyRequestShow.show(createKeyRequest)
    case getPublicKeyRequest: GetPublicKeyRequest =>
      getPublicKeyRequestShow.show(getPublicKeyRequest)
    case describeKeyRequest: DescribeKeyRequest =>
      retrieveKeyMetadataRequestShow.show(describeKeyRequest)
    case encryptRequest: EncryptRequest => encryptRequestShow.show(encryptRequest)
    case decryptRequest: DecryptRequest => decryptRequestShow.show(decryptRequest)
    case signRequest: SignRequest => signRequestShow.show(signRequest)
    case scheduleKeyDeletionRequest: ScheduleKeyDeletionRequest =>
      deleteKeyRequestShow.show(scheduleKeyDeletionRequest)
    case other => other.toString
  }

  implicit val sdkResponsePretty: Show[SdkResponse] = {
    case createKeyResponse: CreateKeyResponse => createKeyResponseShow.show(createKeyResponse)
    case getPublicKeyResponse: GetPublicKeyResponse =>
      getPublicKeyResponseShow.show(getPublicKeyResponse)
    case describeKeyResponse: DescribeKeyResponse =>
      retrieveKeyMetadataResponseShow.show(describeKeyResponse)
    case encryptResponse: EncryptResponse => encryptResponseShow.show(encryptResponse)
    case decryptResponse: DecryptResponse => decryptResponseShow.show(decryptResponse)
    case signResponse: SignResponse => signResponseShow.show(signResponse)
    case scheduleKeyDeletionResponse: ScheduleKeyDeletionResponse =>
      deleteKeyResponseShow.show(scheduleKeyDeletionResponse)
    case response: AwsResponse =>
      s"[${response.responseMetadata().requestId()}] - ${response.toString}"
    case response => response.toString
  }
}
