-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Wrapper around the tests so we can change the arguments
module DA.Daml.Assistant.CreateDamlAppTestsMain (main) where

import DA.Daml.Assistant.CreateDamlAppTests qualified as CreateDamlAppTests

main :: IO ()
main = CreateDamlAppTests.main

