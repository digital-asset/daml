// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

import scopt.Read

object Readers {

  implicit val durationRead: Read[Duration] = new Read[Duration] {
    override def arity: Int = 1

    override val reads: String => Duration = Duration.parse
  }

}
