-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table blocks (
    id bigint primary key,
    request binary large object not null,
    uuid varchar unique not null
);
