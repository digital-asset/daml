// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput, baseEndpoint}
import com.digitalasset.canton.ledger.client.services.version.VersionClient
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import sttp.tapir.{AnyEndpoint, Endpoint, stringBody}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class JsApiDocsService(
    versionClient: VersionClient,
    endpointDescriptions: List[AnyEndpoint],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends Endpoints {
  private val apidocsGenerator = new ApiDocsGenerator(loggerFactory)
  private lazy val docs: Endpoint[CallerContext, Unit, Unit, Unit, Any] =
    baseEndpoint.in(sttp.tapir.stringToPath("docs"))

  // The cache is shared for all tokens - assumption is that api documentation is public
  private val apiDocsCache = new AtomicReference[Option[ApiDocs]](None)

  def endpoints() = List(
    withTraceHeaders(
      docs.get
        .in(sttp.tapir.stringToPath("openapi"))
        .description("OpenAPI documentation")
    )
      .out(stringBody)
      .serverSecurityLogicSuccess(Future.successful)
      .serverLogicSuccess(caller =>
        (in: TracedInput[Unit]) => getApiDocs(caller.token())(in.traceContext).map(_.openApi)
      ),
    withTraceHeaders(
      docs.get
        .in(sttp.tapir.stringToPath("asyncapi"))
        .description("AsyncAPI documentation")
    )
      .out(stringBody)
      .serverSecurityLogicSuccess(Future.successful)
      .serverLogicSuccess(caller =>
        (in: TracedInput[Unit]) => getApiDocs(caller.token())(in.traceContext).map(_.asyncApi)
      ),
  )

  private def getApiDocs(
      token: Option[String]
  )(implicit traceContext: TraceContext): Future[ApiDocs] =
    apiDocsCache.get().map(Future.successful(_)).getOrElse {
      for {
        version <- versionClient.getApiVersion(token)
        apidocs = apidocsGenerator.createDocs(version, endpointDescriptions)
        _ = apiDocsCache.set(Some(apidocs))
      } yield apidocs
    }

}
