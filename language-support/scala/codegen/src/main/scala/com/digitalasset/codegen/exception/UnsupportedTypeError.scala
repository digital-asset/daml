// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.exception

/**
  * The reason why a given type's code can't be generated
  */
final case class UnsopportedTypeError(msg: String)
