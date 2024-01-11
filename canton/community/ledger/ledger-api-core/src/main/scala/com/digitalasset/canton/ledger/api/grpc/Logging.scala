// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import com.daml.logging.entries.LoggingEntry

object Logging {

  def traceId(id: Option[String]): LoggingEntry =
    "tid" -> (id.getOrElse(""): String)

}
