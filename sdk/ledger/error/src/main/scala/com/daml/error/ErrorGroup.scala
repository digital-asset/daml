// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

abstract class ErrorGroup()(implicit parent: ErrorClass) {
  private val simpleClassName: String = getClass.getSimpleName.replace("$", "")
  val fullClassName: String = getClass.getName

  implicit val errorClass: ErrorClass =
    parent.extend(Grouping(docName = simpleClassName, fullClassName = fullClassName))
}
