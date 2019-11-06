// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

class HttpServiceIntegrationTest extends AbstractHttpServiceIntegrationTest {
  override def jdbcConfig: Option[JdbcConfig] = None
}
