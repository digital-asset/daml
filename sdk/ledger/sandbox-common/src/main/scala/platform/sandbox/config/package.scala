// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import scalaz.{@@, Tag}

package config {

  sealed trait LedgerNameTag

}

package object config {

  type LedgerName = String @@ LedgerNameTag
  val LedgerName: Tag.TagOf[LedgerNameTag] = Tag.of[LedgerNameTag]

}
