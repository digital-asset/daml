// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

object ExceptionOps {

  implicit class ExceptionOps(val underlying: Throwable) extends AnyVal {
    def description: String = getDescription(underlying)
  }

  def getDescription(e: Throwable): String = {
    val name: String = getClass.getName
    Option(e.getMessage).filter(_.nonEmpty) match {
      case Some(m) => s"$name: $m"
      case None => name
    }
  }
}
