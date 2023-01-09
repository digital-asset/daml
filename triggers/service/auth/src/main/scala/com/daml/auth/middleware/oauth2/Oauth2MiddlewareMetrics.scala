// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.http.DamlHttpMetrics

case class Oauth2MiddlewareMetrics(factory: Factory) {

  val http = new DamlHttpMetrics(factory, "oauth2-middleware")

}
