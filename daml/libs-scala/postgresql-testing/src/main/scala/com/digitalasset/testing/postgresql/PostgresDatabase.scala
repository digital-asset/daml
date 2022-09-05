// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import com.daml.ports.Port

final case class PostgresDatabase private[postgresql] (
    private val server: PostgresServer,
    databaseName: String,
) {
  def hostName: String = server.hostName

  def port: Port = server.port

  def userName: String = server.userName

  def password: String = server.password

  def urlWithoutCredentials: String =
    s"jdbc:postgresql://$hostName:$port/$databaseName"

  def url: String =
    s"$urlWithoutCredentials?user=$userName&password=$password"

  override def toString: String = url
}
