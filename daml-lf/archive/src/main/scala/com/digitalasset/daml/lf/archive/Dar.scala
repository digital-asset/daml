// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

case class Dar[A](main: A, dependencies: List[A]) {
  def all: List[A] = main :: dependencies
}
