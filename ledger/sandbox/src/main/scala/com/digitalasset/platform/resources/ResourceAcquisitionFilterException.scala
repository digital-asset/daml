// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import java.io.IOException

final class ResourceAcquisitionFilterException
    extends IOException("Predicate failed during resource acquisition.", null)
