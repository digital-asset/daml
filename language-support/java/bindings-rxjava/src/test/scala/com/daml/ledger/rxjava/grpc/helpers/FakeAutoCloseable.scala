// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

private[helpers] trait FakeAutoCloseable extends AutoCloseable {

  override final def close(): Unit = ()

}
