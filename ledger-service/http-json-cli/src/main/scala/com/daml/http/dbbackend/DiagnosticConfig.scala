// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package dbbackend

import com.daml.dbutils

import java.io.File

private[http] final case class DiagnosticConfig(
    baseConfig: dbutils.JdbcConfig,
    clientInfo: Option[String],
    query: File,
)
