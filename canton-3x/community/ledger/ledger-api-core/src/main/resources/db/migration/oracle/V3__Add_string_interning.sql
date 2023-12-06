--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

CREATE TABLE string_interning (
    internal_id      NUMBER          PRIMARY KEY NOT NULL,
    external_string  VARCHAR2(4000)
);