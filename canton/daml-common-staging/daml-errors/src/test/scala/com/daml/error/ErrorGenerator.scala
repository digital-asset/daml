// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import org.scalacheck.{Arbitrary, Gen}

object ErrorGenerator {
  final case class RichTestError(
      errorCode: ErrorCode,
      correlationId: Option[String] = None,
      traceId: Option[String] = None,
      contextMap: Map[String, Any] = Map(),
      loggingProperties: Map[String, String] = Map(),
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
      override val definiteAnswerO: Option[Boolean] = None,
      override val resources: Seq[(ErrorResource, String)] = Seq(),
  ) extends DamlError(
        cause,
        throwableO,
        contextMap,
      )(errorCode, new NoLogging(loggingProperties, correlationId, traceId))

  private final case class TestErrorCode(override val id: String, errorCategory: ErrorCategory)
      extends ErrorCode(id, errorCategory)(ErrorClass.root()) {}

  private[error] def asciiPrintableStrOfN(maxSize: Int) = for {
    chars <- Gen.listOfN(maxSize, Gen.alphaNumChar)
  } yield chars.mkString

  private val errorResourceGen = for {
    typ <- asciiPrintableStrOfN(256).map(ErrorResource(_))
    msg <- asciiPrintableStrOfN(1024)
  } yield (typ, msg)

  private val contextMapGen = for {
    contextMap <- Gen.mapOfN(
      50,
      for {
        k <- asciiPrintableStrOfN(256)
        v <- asciiPrintableStrOfN(256)
      } yield (k, v),
    )
  } yield contextMap

  val defaultErrorGen: Gen[RichTestError] = errorGenerator(None)

  def errorGenerator(
      securitySensitive: Option[Boolean],
      additionalErrorCategoryFilter: ErrorCategory => Boolean = _ => true,
  ): Gen[RichTestError] =
    for {
      errorCodeId <- Gen.listOfN(63, Gen.alphaUpperChar).map(_.mkString)
      category <- Gen.oneOf(
        securitySensitive
          .fold(ErrorCategory.all)(securitySensitive =>
            ErrorCategory.all.filter(_.securitySensitive == securitySensitive)
          )
          .filter(additionalErrorCategoryFilter)
      )
      errorCode = TestErrorCode(errorCodeId, category)
      correlationId <- asciiPrintableStrOfN(
        SerializableErrorCodeComponents.MaxTraceIdCorrelationIdSize
      ).map(Option(_).filter(_.nonEmpty))
      traceId <- asciiPrintableStrOfN(SerializableErrorCodeComponents.MaxTraceIdCorrelationIdSize)
        .map(Option(_).filter(_.nonEmpty))
      message <- asciiPrintableStrOfN(2000)
      definiteAnswerO <- Arbitrary.arbitrary[Option[Boolean]]
      errorResources <- Gen.listOfN(50, errorResourceGen)
      extraContextMap <- contextMapGen
      loggingProperties <- contextMapGen
      throwableO <- Gen.option(Gen.asciiPrintableStr.map(new RuntimeException(_)))
    } yield RichTestError(
      errorCode = errorCode,
      correlationId = correlationId,
      traceId = traceId,
      contextMap = extraContextMap,
      loggingProperties = loggingProperties,
      cause = message,
      definiteAnswerO = definiteAnswerO,
      throwableO = throwableO,
      resources = errorResources,
    )
}
