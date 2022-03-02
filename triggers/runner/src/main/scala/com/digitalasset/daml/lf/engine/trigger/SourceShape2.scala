// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.stream.{Inlet, Outlet, Shape}

import scala.collection.immutable.Seq

private[trigger] final case class SourceShape2[L, R](out1: Outlet[L], out2: Outlet[R])
    extends Shape {
  override val inlets: Seq[Inlet[_]] = Seq.empty
  override val outlets: Seq[Outlet[_]] = Seq(out1, out2)
  override def deepCopy() = copy(out1.carbonCopy(), out2.carbonCopy())
}
