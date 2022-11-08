// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import com.daml.metrics.api.MetricsContext

object LabelExtractor {

  def labelsFromRequest(request: HttpRequest): MetricsContext = {
    val baseLabels = Map[String, String](
      ("http_verb", request.method.name),
      ("path", request.uri.path.toString),
    )
    val host = request.uri.authority.host
    val labelsWithHost =
      if (host.isEmpty)
        baseLabels
      else
        baseLabels + (("host", host.address))
    MetricsContext(labelsWithHost)
  }

  def addLabelsFromResponse(
      metricsContext: MetricsContext,
      response: HttpResponse,
  ): MetricsContext = {
    metricsContext.merge(
      MetricsContext(
        Map[String, String](("http_status", response.status.intValue.toString))
      )
    )

  }

}
