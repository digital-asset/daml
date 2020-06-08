-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Migration.Types
  ( Test(..)
  , SdkVersion(..)
  ) where

data Test s r = Test
  { executeStep :: SdkVersion -> String -> Int -> s -> IO r
  -- ^ Execute a step given SDK version, the host and port
  -- and current state and produce a result.
  , validateStep :: SdkVersion -> s -> r -> Either String s
  -- ^ Validate the result of a step given the SDK version and current state
  -- and produce either an error or a new state.
  , initialState :: s
  -- ^ The initial state to start with.
  }

newtype SdkVersion = SdkVersion { getSdkVersion :: String }

