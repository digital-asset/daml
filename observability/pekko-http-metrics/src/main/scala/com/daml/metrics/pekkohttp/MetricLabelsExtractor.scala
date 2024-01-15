// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.pekkohttp

import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import com.daml.metrics.http.Labels._

object MetricLabelsExtractor {

  def labelsFromRequest(request: HttpRequest): Seq[(String, String)] = {
    val baseLabels = Seq[(String, String)](
      (HttpVerbLabel, request.method.name),
      (PathLabel, request.uri.path.toString),
    )
    val host = request.uri.authority.host
    val labelsWithHost =
      if (host.isEmpty)
        baseLabels
      else
        ((HostLabel, host.address)) +: baseLabels
    labelsWithHost
  }

  def labelsFromResponse(
      response: HttpResponse
  ): Seq[(String, String)] = {
    Seq((HttpStatusLabel, response.status.intValue.toString))
  }

}
