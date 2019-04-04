-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
module DA.Sdk.Cli.Version where

import Data.Text (Text)
version :: Text
#ifdef DA_CLI_VERSION
version = DA_CLI_VERSION
#else
version = "HEAD"
#endif
