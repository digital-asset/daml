-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V26.2: Set participant_contracts.create_argument to be non-nullable
---------------------------------------------------------------------------------------------------

alter table participant_contracts
    alter column template_id set not null,
    alter column create_argument set not null;
