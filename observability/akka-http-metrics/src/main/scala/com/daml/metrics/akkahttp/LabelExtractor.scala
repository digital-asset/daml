// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.http.scaladsl.model.{HttpRequest, HttpResponse}

object LabelExtractor {

  def labelsFromRequest(request: HttpRequest): Seq[(String, String)] = {
    val baseLabels = Seq[(String, String)](
      ("http_verb", request.method.name),
      ("path", request.uri.path.toString),
    )
    val host = request.uri.authority.host
    val labelsWithHost =
      if (host.isEmpty)
        baseLabels
      else
        (("host", host.address)) +: baseLabels
    labelsWithHost
  }

  def labelsFromResponse(
      response: HttpResponse
  ): Seq[(String, String)] = {
    Seq(("http_status", response.status.intValue.toString))
  }

}
