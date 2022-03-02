// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.archives

import com.google.protobuf.ByteString

/** Stores [[com.daml.daml_lf_dev.DamlLf.Archive]] as a [[ByteString]]. */
case class RawArchive(byteString: ByteString) extends AnyVal
