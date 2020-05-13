// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import com.daml.ports.Port

final case class PostgresDatabase(
    hostName: String,
    port: Port,
    userName: String,
    databaseName: String,
) {
  def url: String = s"jdbc:postgresql://$hostName:$port/$databaseName?user=$userName"

  override def toString: String = url
}
