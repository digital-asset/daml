// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

trait NoCopy {
  // prevents autogeneration of copy method in case class
  protected def copy(nothing: Nothing): Nothing = nothing
}
