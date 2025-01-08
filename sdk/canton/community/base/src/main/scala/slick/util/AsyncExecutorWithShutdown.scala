// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slick.util

trait AsyncExecutorWithShutdown extends AsyncExecutor {
  def isShuttingDown: Boolean
}
