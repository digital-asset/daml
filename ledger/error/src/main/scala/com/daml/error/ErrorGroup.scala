// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

abstract class ErrorGroup()(implicit parent: ErrorClass) {
  implicit val errorClass: ErrorClass = parent.extend(getClass.getSimpleName.replace("$", ""))
}
