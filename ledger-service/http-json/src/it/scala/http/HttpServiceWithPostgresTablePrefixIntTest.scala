// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceWithPostgresTablePrefixIntTest extends HttpServiceWithPostgresIntTest {
  override def jdbcConfig_ = super.jdbcConfig_.copy(tablePrefix = "some_nice_prefix_")
}
