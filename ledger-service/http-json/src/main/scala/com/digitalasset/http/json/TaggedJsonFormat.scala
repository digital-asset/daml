// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import scalaz.{@@, Tag}
import spray.json.JsonFormat

object TaggedJsonFormat {
  def taggedJsonFormat[A: JsonFormat, T]: JsonFormat[A @@ T] = Tag.subst(implicitly[JsonFormat[A]])
}
