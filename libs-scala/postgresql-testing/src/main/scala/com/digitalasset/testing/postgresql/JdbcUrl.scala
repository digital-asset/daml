// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

final case class JdbcUrl(url: String) extends AnyVal {
  override def toString: String = url
}
