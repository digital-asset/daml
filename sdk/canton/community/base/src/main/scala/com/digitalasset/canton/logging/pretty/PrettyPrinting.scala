// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

import com.digitalasset.canton.util.ShowUtil

/** Extend this trait to directly enable pretty printing via supertype.
  */
trait PrettyPrinting extends ShowUtil with PrettyUtil {

  /** Indicates how to pretty print this instance.
    * See `PrettyPrintingTest` for examples on how to implement this method.
    */
  protected[pretty] def pretty: Pretty[this.type]

  /** Yields a readable string representation based on [[com.digitalasset.canton.logging.pretty.Pretty.DefaultPprinter]].
    * `Final` to avoid accidental overwriting.
    */
  // Do not cache the toString representation because it could be outdated in classes with mutable state
  override final def toString: String = {
    // Special construction here to fail gracefully if this is a mocked instance.
    Pretty.PrettyOps[this.type](this)(pretty).toPrettyString()
  }
}
