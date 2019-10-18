// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.dbbackend

import cats.effect.{ContextShift, IO}
import doobie._

object DbConnection {

  type T = Transactor.Aux[IO, Unit]

  def connect(jdbcDriver: String, jdbcUrl: String, username: String, password: String)(
      implicit cs: ContextShift[IO]): T =
    Transactor
      .fromDriverManager[IO](jdbcDriver, jdbcUrl, username, password)(IO.ioConcurrentEffect(cs), cs)
}
