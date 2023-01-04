// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

object ExceptionOps {

  implicit class ExceptionOps(val underlying: Throwable) extends AnyVal {
    def description: String = getDescription(underlying)
  }

  def getDescription(e: Throwable): String = {
    val name: String = e.getClass.getName
    Option(e.getMessage).filter(_.nonEmpty) match {
      case Some(m) => s"$name: $m"
      case None => name
    }
  }
}
