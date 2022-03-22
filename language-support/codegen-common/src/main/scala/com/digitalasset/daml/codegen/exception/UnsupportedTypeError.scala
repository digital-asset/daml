// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.exception

/** The reason why a given type's code can't be generated
  */
final case class UnsupportedTypeError(msg: String)
