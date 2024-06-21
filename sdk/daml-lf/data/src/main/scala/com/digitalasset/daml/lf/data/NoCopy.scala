// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

trait NoCopy {
  // prevents autogeneration of copy method in case class
  protected def copy(nothing: Nothing): Nothing = nothing
}
