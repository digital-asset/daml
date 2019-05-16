-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module PastAndFuture(PastAndFuture(..)) where

import Stream

data PastAndFuture a = PF { past :: [a], future :: Stream a }
