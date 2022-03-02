-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Wrapper around the tests so we can change the arguments
module DA.Daml.Assistant.CreateDamlAppTestsMain (main) where

import qualified DA.Daml.Assistant.CreateDamlAppTests as CreateDamlAppTests

main :: IO ()
main = CreateDamlAppTests.main

