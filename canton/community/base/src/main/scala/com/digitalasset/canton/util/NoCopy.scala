// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

/** Prevents auto-generation of the copy method in a case class.
  * Case classes with private constructors typically shouldn't have a copy method.
  */
trait NoCopy {
  protected def copy(nothing: Nothing): Nothing = nothing
}
