// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.dropwizard.FactoryWithDBMetrics

@MetricDoc.GroupTag(
  representative = "daml.user_management.<operation>.wait"
)
@MetricDoc.GroupTag(
  representative = "daml.user_management.<operation>.exec"
)
@MetricDoc.GroupTag(
  representative = "daml.user_management.<operation>.translation"
)
@MetricDoc.GroupTag(
  representative = "daml.user_management.<operation>.compression"
)
@MetricDoc.GroupTag(
  representative = "daml.user_management.<operation>.commit"
)
@MetricDoc.GroupTag(
  representative = "daml.user_management.<operation>.query"
)
class UserManagementMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends FactoryWithDBMetrics {

  val cache = new CacheMetrics(prefix :+ "cache", registry)

  val getUserInfo: DatabaseMetrics = createDbMetrics("get_user_info")
  val createUser: DatabaseMetrics = createDbMetrics("create_user")
  val deleteUser: DatabaseMetrics = createDbMetrics("delete_user")
  val updateUser: DatabaseMetrics = createDbMetrics("update_user")
  val grantRights: DatabaseMetrics = createDbMetrics("grant_rights")
  val revokeRights: DatabaseMetrics = createDbMetrics("revoke_rights")
  val listUsers: DatabaseMetrics = createDbMetrics("list_users")
}
