// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

/** Trait for a group of schedulers started and stopped as a group.
  */
trait Schedulers extends StartStoppable with AutoCloseable {

  /** Accessor for individual scheduler by name
    * Throws an IllegalStateException if a scheduler is looked up that does not exist
    */
  def get(name: String): Option[Scheduler]
}
