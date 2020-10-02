// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.effect.{ContextShift, IO}
import doobie._

object Connection {

  type T = Transactor.Aux[IO, Unit]

  def connect(jdbcDriver: String, jdbcUrl: String, username: String, password: String)(
      implicit cs: ContextShift[IO]): T =
    Transactor
      .fromDriverManager[IO](jdbcDriver, jdbcUrl, username, password)(IO.ioConcurrentEffect(cs), cs)
}
