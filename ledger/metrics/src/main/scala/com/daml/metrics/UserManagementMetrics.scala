// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.{MetricRegistry}

class UserManagementMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.FactoryWithDBMetrics {

  val cache = new CacheMetrics(prefix :+ "cache", registry)

  val getUserInfo: DatabaseMetrics = createDbMetrics("get_user_info")
  val createUser: DatabaseMetrics = createDbMetrics("create_user")
  val deleteUser: DatabaseMetrics = createDbMetrics("delete_user")
  val updateUser: DatabaseMetrics = createDbMetrics("update_user")
  val grantRights: DatabaseMetrics = createDbMetrics("grant_rights")
  val revokeRights: DatabaseMetrics = createDbMetrics("revoke_rights")
  val listUsers: DatabaseMetrics = createDbMetrics("list_users")
}
