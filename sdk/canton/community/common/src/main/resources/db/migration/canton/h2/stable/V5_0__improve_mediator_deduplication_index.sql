-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop index idx_mediator_deduplication_store_expire_after;
-- Invert the order of the index attributes
create index idx_mediator_deduplication_store_expire_after on mediator_deduplication_store(mediator_id, expire_after);
