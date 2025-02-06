// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

@SuppressWarnings(Array("org.wartremover.warts.Null"))
private[conflictdetection] final case class IllegalConflictDetectionStateException(
    msg: String,
    cause: Throwable = null,
) extends RuntimeException(msg, cause)
