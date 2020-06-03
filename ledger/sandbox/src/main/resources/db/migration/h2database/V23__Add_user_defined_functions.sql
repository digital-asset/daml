-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V34: Add user-defined functions
--
-- Add `da_varchar_arrays_intersection` returns intersection of two passed varchar arrays (`varchar[]`).
-- Add `da_do_varchar_arrays_intersect` returns `boolean` if two passed varchar arrays (`varchar[]`) has intersection.
---------------------------------------------------------------------------------------------------

drop alias array_intersection;

create alias da_varchar_arrays_intersection for "com.daml.platform.store.dao.events.SqlFunctions.varcharArraysIntersection";

create alias da_do_varchar_arrays_intersect for "com.daml.platform.store.dao.events.SqlFunctions.doVarcharArraysIntersect";
