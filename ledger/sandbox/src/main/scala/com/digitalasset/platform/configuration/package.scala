// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform

import scalaz.{@@, Tag}

package object configuration {

  sealed trait ServerNameTag

  type ServerName = String @@ ServerNameTag
  val ServerName: Tag.TagOf[ServerNameTag] = Tag.of[ServerNameTag]

}
