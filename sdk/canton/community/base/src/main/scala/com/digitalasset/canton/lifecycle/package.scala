// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

package object lifecycle {

  /** The monad combination of [[scala.concurrent.Future]] with [[UnlessShutdown]] as an abstract type
    *
    * @see FutureUnlessShutdownSig.Ops for extension methods on the abstract type
    */
  type FutureUnlessShutdown[+A] = FutureUnlessShutdownImpl.Instance.FutureUnlessShutdown[A]
}
