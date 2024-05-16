// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import com.digitalasset.canton.BaseTestWordSpec

class BootstrapStageTest extends BaseTestWordSpec {

  "implement me" in {
    // TODO(#12941) implement me
    succeed
  }

  "active node" should {
    "perform auto-init with multiple stages" in {
      // TODO(#12941) implement me
      succeed
    }
    "wait for a manual input stage for startup" in {
      // TODO(#12941) implement me
      succeed
    }
    "recover from a crash during initialisation" in {
      // TODO(#12941) implement me
      succeed
    }
    "restart with multiple stages without rerunning init" in {
      // TODO(#12941) implement me
      succeed
    }
    "cleanly abort initialisation if one stage fails" in {
      // TODO(#12941) implement me (test that auto-close happens on all places)
      succeed
    }
    "ensure user interaction does not race" in {
      // TODO(#12941) implement me
      succeed
    }
  }
  "passive node" should {
    "wait for active replica to complete each init step" in {
      // TODO(#12941) implement me
      succeed
    }
    "pick up manual init steps from active replica" in {
      // TODO(#12941) implement me
      succeed
    }
    "take over initialisation if active node became passive" in {
      // TODO(#12941) implement me
      succeed
    }
    "be graceful if it became passive during init" in {
      // TODO(#12941) implement me: if the node becomes passive during init, we should
      //         gracefully wait for active node to finish the init
      succeed
    }
  }

}
