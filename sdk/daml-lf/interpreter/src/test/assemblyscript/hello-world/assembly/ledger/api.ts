// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as internal from "./internal";

export function logInfo(msg: string): void {
  let msgByteStr = internal.ByteString.fromString(msg);
  msgByteStr.alloc();
  internal.logInfo(msgByteStr.heapPtr());
  msgByteStr.dealloc();
}

export function createContract(): lf.Identifier {
}
