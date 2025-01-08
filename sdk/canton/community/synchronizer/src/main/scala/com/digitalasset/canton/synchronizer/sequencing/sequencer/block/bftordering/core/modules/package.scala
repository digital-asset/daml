// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core

package object modules {

  /** Produces the short name of a Scala class for logging purposes, e.g.,
    * `Availability.Dissemination.MempoolCreatedBatch`.
    */
  private[modules] def shortType(o: Object): String =
    o.getClass.getName.split('.').toSeq.reverse.headOption.map(_.replace("$", ".")).getOrElse("")
}
