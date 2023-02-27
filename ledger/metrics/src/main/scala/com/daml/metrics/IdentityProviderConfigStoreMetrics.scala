// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricHandle.{LabeledMetricsFactory, MetricsFactory}
import com.daml.metrics.api.MetricName

class IdentityProviderConfigStoreMetrics(
    prefix: MetricName,
    factory: MetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
) extends DatabaseMetricsFactory(prefix, labeledMetricsFactory) {

  val idpConfigCache = new CacheMetrics(prefix :+ "idp_config_cache", factory)
  val verifierCache = new CacheMetrics(prefix :+ "verifier_cache", factory)
  val createIdpConfig: DatabaseMetrics = createDbMetrics("create_identity_provider_config")
  val getIdpConfig: DatabaseMetrics = createDbMetrics("get_identity_provider_config")
  val deleteIdpConfig: DatabaseMetrics = createDbMetrics("delete_identity_provider_config")
  val updateIdpConfig: DatabaseMetrics = createDbMetrics("update_identity_provider_config")
  val listIdpConfigs: DatabaseMetrics = createDbMetrics("list_identity_provider_configs")

}
