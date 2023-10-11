--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

CREATE TABLE string_interning (
    internal_id     INTEGER PRIMARY KEY NOT NULL,
    external_string TEXT
);