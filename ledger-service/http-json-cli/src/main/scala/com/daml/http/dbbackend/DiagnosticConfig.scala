// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package dbbackend

import com.daml.dbutils
import scala.concurrent.duration._

import java.io.File
import scala.concurrent.duration.FiniteDuration

private[http] final case class DiagnosticConfig(
    baseConfig: dbutils.JdbcConfig,
    query: File,
    minDelay: FiniteDuration = 60.seconds,
    clientInfoName: Option[String] = None,
)
