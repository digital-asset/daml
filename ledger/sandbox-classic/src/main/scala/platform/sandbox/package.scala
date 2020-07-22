// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import scalaz.{@@, Tag}

package sandbox {

  sealed trait LedgerNameTag

}

package object sandbox {

  type LedgerName = String @@ LedgerNameTag
  val LedgerName: Tag.TagOf[LedgerNameTag] = Tag.of[LedgerNameTag]

}
