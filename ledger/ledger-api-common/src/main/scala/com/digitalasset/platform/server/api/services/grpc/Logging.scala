// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import com.daml.logging.entries.LoggingEntry

object Logging {

  def traceId(id: Option[String]): LoggingEntry =
    "tid" -> (id.getOrElse(""): String)

}
