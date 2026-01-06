// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.CancellableEvent

private[bftordering] class FakeCancellableEvent(onCancel: () => Boolean = () => true)
    extends CancellableEvent {
  override def cancel()(implicit metricsContext: MetricsContext): Boolean = onCancel()
}
