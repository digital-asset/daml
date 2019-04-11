-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module Rattle(rattle, CmdOption(..), cmd, cmd_, Stdout(..), Exit(..), Stderr(..)) where

import Development.Shake.Command

rattle :: IO () -> IO ()
rattle act = act
