// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

import io.netty.handler.ssl.ClientAuth
import scopt.Read

object Readers {

  implicit val durationRead: Read[Duration] = new Read[Duration] {
    override def arity: Int = 1

    override val reads: String => Duration = Duration.parse
  }

  implicit val clientAuthRead: Read[ClientAuth] = Read.reads {
    case "none" => ClientAuth.NONE
    case "optional" => ClientAuth.OPTIONAL
    case "require" => ClientAuth.REQUIRE
    case _ =>
      throw new InvalidConfigException(s"""Must be one of "none", "optional", or "require".""")
  }

}
