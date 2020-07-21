-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V36: Add user-defined functions
--
-- Add `da_varchar_arrays_intersection` returns intersection of two passed varchar arrays (`varchar[]`).
-- Add `da_do_varchar_arrays_intersect` returns `boolean` if two passed varchar arrays (`varchar[]`) has intersection.
---------------------------------------------------------------------------------------------------

create function da_varchar_arrays_intersection(varchar[], varchar[]) returns varchar[] as $$
    select array(select unnest($1) intersect select unnest($2))
$$ language SQL;

create function da_do_varchar_arrays_intersect(varchar[], varchar[]) returns boolean as $$
    select $1 && $2
$$ language SQL;
