// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.ledger.api.domain.ObjectMeta
import com.daml.ledger.api.v1.{admin => proto_admin}

object Utils {
  def toProtoObjectMeta(meta: ObjectMeta): proto_admin.object_meta.ObjectMeta =
    proto_admin.object_meta.ObjectMeta(
      resourceVersion = serializeResourceVersion(meta.resourceVersionO),
      annotations = meta.annotations,
    )

  private def serializeResourceVersion(resourceVersionO: Option[Long]): String =
    resourceVersionO.fold("")(_.toString)
}
