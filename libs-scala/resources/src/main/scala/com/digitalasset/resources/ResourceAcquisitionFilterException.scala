// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.io.IOException

final class ResourceAcquisitionFilterException
    extends IOException("Predicate failed during resource acquisition.", null)
