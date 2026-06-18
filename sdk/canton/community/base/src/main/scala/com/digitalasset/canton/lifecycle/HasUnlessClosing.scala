// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

trait HasUnlessClosing {

  /** Returns whether the component is closing or has already been closed
    */
  def isClosing: Boolean

  /** Runs the computation `fa` unless [[isClosing]] returns true.
    *
    * This method does not delay the closing while `fa` is running, unlike the methods in
    * `HasSynchronizeWithClosing`. Accordingly, this method is useful for intermittent checks
    * whether the result of the computation is still relevant.
    *
    * @return
    *   The result of `fa` or
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if [[isClosing]]
    *   is true
    */
  @inline
  final def unlessClosing[F[_], A](fa: => F[A])(implicit F: CanAbortDueToShutdown[F]): F[A] =
    if (isClosing) F.abort else fa
}
