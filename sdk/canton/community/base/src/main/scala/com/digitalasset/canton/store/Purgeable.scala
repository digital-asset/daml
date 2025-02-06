// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext

/* Interface for a store that can be entirely purged once none of the data is needed anymore.
 */
trait Purgeable {

  /** Purges all data from the store. This MUST ONLY be invoked when none of the data is needed anymore
    * for example on synchronizer migration once all the data has been reassigned to the new synchronizer.
    */
  def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}
