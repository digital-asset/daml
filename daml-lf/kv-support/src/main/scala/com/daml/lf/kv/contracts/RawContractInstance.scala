// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.contracts

import com.google.protobuf.ByteString

case class RawContractInstance(byteString: ByteString) extends AnyVal
