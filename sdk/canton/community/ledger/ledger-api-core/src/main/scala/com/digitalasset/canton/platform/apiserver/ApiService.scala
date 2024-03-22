// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.digitalasset.canton.config.RequireTypes.Port

trait ApiService {

  /** the API port the server is listening on */
  def port: Port

}
