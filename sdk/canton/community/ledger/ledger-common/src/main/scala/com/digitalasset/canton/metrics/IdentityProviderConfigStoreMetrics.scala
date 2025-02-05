// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName
import com.daml.metrics.{CacheMetrics, DatabaseMetrics}

// Private constructor to avoid being instantiated multiple times by accident
final class IdentityProviderConfigStoreMetrics private[metrics] (
    prefix: MetricName,
    labeledMetricsFactory: LabeledMetricsFactory,
) extends DatabaseMetricsFactory(prefix, labeledMetricsFactory) {

  val idpConfigCache: CacheMetrics =
    new CacheMetrics(prefix :+ "idp_config_cache", labeledMetricsFactory)
  val verifierCache: CacheMetrics =
    new CacheMetrics(prefix :+ "verifier_cache", labeledMetricsFactory)
  val createIdpConfig: DatabaseMetrics = createDbMetrics("create_identity_provider_config")
  val getIdpConfig: DatabaseMetrics = createDbMetrics("get_identity_provider_config")
  val deleteIdpConfig: DatabaseMetrics = createDbMetrics("delete_identity_provider_config")
  val updateIdpConfig: DatabaseMetrics = createDbMetrics("update_identity_provider_config")
  val listIdpConfigs: DatabaseMetrics = createDbMetrics("list_identity_provider_configs")

}
