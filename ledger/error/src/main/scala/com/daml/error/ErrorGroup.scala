// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

abstract class ErrorGroup()(implicit parent: ErrorClass) {
  private val fullClassName: String = getClass.getName
  // Hit https://github.com/scala/bug/issues/5425?orig=1 here: we cannot use .getSimpleName in deeply nested objects
  // TODO error codes: Switch to using .getSimpleName when switching to JDK 9+
  implicit val errorClass: ErrorClass =
    parent.extend(fullClassName.substring(fullClassName.lastIndexOf("$")))
}
