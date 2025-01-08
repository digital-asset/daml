// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.unit

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.{
  CancellableEvent,
  Env,
  ModuleRef,
}
import org.scalatest.Assertions.fail

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

package object modules {
  def fakeIgnoringModule[MessageT]: ModuleRef[MessageT] = new FakeIgnoringModuleRef()

  private[unit] def fakeCellModule[ModuleMessageT, CellMessageT <: ModuleMessageT: Manifest](
      cell: AtomicReference[Option[CellMessageT]]
  ): ModuleRef[ModuleMessageT] = {
    case cellMsg: CellMessageT => cell.set(Some(cellMsg))
    case other => fail(s"Unexpected message $other")
  }

  private[unit] def fakeRecordingModule[MessageT](
      buffer: mutable.ArrayBuffer[MessageT]
  ): ModuleRef[MessageT] =
    msg => buffer += msg

  private[unit] def fakeModuleExpectingSilence[MessageT]: ModuleRef[MessageT] =
    m => fail(s"Module should not receive any requests but received $m")

  private[unit] def fakeCancellableEventExpectingSilence: CancellableEvent =
    () => fail("Module should not cancel delayed event")

  private[unit] def fakeCryptoProvider[E <: Env[E]]: CryptoProvider[E] = new FakeCryptoProvider()
}
