// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.LfPackageId
import com.google.protobuf.ByteString

final case class UploadDarData(
    bytes: ByteString,
    description: Option[String],
    expectedMainPackageId: Option[LfPackageId],
)
