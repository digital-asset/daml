// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

trait HasDocumentedMetrics {

  /** Ensure that metrics get populated
    *
    * Many metrics objects are lazily initialized, so this method is used to ensure that they are populated.
    * This can also be used to create specific metrics for documentation purposes that are otherwise
    * not used.
    */
  def docPoke(): Unit = {}
}
