// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef

class FakeIgnoringModuleRef[MessageT] extends ModuleRef[MessageT] {
  override def asyncSend(msg: MessageT): Unit = ()
}
