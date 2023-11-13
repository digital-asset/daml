// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.api.v1

import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import com.daml.nonrepudiation.api.Result
import com.daml.nonrepudiation.{CommandIdString, SignedPayload, SignedPayloadRepository}
import com.google.common.io.BaseEncoding
import org.slf4j.{Logger, LoggerFactory}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.collection.immutable.ArraySeq

private[api] final class SignedPayloadsEndpoint private (
    signedPayloads: SignedPayloadRepository.Read[CommandIdString]
) extends Result.JsonProtocol
    with SignedPayloadsEndpoint.JsonProtocol {

  import SignedPayloadsEndpoint._

  private val route: Route =
    path(Segment.map(CommandIdString.wrap)) { commandId =>
      get {
        handleExceptions(logAndReport(logger)(UnableToRetrieveTheSignedPayload)) {
          val responses = signedPayloads.get(commandId).map(toResponse)
          if (responses.nonEmpty) {
            complete(Result.Success(responses, 200))
          } else {
            reject
          }
        }
      }
    }

}

object SignedPayloadsEndpoint {

  def apply(signedPayloads: SignedPayloadRepository.Read[CommandIdString]): Route =
    new SignedPayloadsEndpoint(signedPayloads).route

  private val logger: Logger = LoggerFactory.getLogger(classOf[SignedPayloadsEndpoint])

  private[api] val UnableToRetrieveTheSignedPayload: String =
    "An error occurred when trying to retrieve the signed payload, please try again."

  final case class Response(
      algorithm: String,
      fingerprint: String,
      payload: String,
      signature: String,
      timestamp: Long,
  )

  private def base64Url(bytes: ArraySeq.ofByte): String =
    BaseEncoding.base64Url().encode(bytes.unsafeArray)

  def toResponse(signedPayload: SignedPayload): Response =
    Response(
      algorithm = signedPayload.algorithm,
      fingerprint = base64Url(signedPayload.fingerprint),
      payload = base64Url(signedPayload.payload),
      signature = base64Url(signedPayload.signature),
      timestamp = signedPayload.timestamp.toEpochMilli,
    )

  private[api] trait JsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {

    protected implicit val apiSignedPayloadFormat: RootJsonFormat[Response] =
      jsonFormat5(Response.apply)

  }

}
