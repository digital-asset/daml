// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

object ErrorMessages {
  def cannotResolveTemplateId[A](t: domain.TemplateId[A]): String =
    s"Cannot resolve template ID, given: ${t.toString}"
}
