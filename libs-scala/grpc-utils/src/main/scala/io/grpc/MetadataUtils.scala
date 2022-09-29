// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package io.grpc

object MetadataUtils {
  def copy(original: Metadata): Metadata = new Metadata(original.serialize(): _*)
}
