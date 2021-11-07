// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

class LooseSyncChannel {
  private var subscriber: Any => Unit = _

  def push(a: Any): Unit = synchronized {
    if (subscriber != null)
      try {
        subscriber(a)
      } catch {
        case _: Throwable => () // all ok
      }
  }

  def subscribe(subscriber: Any => Unit): Unit = synchronized {
    this.subscriber = subscriber
  }

}
