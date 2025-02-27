// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

/** Note: code in this package is a translation to scala2 of code from
  * https://github.com/DACH-NY/transcode
  */
package object daml_lf {

  /** Collect sequence of [[SchemaEntity]] instances into usable result. See [[Dictionary.collect]]
    * and [[SchemaEntity.collect]] for several examples
    */
  type CollectResult[A, B] = Seq[SchemaEntity[A]] => B
}
