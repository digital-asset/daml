// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v1.{admin as proto_admin}
import com.digitalasset.canton.ledger.api.domain.ObjectMeta

object Utils {
  def toProtoObjectMeta(meta: ObjectMeta): proto_admin.object_meta.ObjectMeta =
    proto_admin.object_meta.ObjectMeta(
      resourceVersion = serializeResourceVersion(meta.resourceVersionO),
      annotations = meta.annotations,
    )

  private def serializeResourceVersion(resourceVersionO: Option[Long]): String =
    resourceVersionO.fold("")(_.toString)
}
