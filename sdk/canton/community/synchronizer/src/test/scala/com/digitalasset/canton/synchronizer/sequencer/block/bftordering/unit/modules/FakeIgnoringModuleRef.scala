// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.tracing.TraceContext

class FakeIgnoringModuleRef[MessageT] extends ModuleRef[MessageT] {
  override def asyncSendTraced(msg: MessageT)(implicit traceContext: TraceContext): Unit = ()
}
