// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.security.MessageDigest

import com.daml.lf.data.Ref.Party

object DeduplicationKeyHashing {
  def hashSubmitters(submitters: List[Party]): String = {
    MessageDigest
      .getInstance("SHA-256")
      .digest(submitters.mkString.getBytes)
      .mkString
  }
}
