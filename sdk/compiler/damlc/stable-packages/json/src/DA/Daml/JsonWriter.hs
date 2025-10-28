-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main(main) where

import qualified Data.Aeson.Encode.Pretty as Aeson
import qualified Data.ByteString.Lazy     as LBS
import qualified Data.ByteString          as BS
import qualified Data.Text                as T
import qualified Data.Yaml                as Yaml

import           DA.Daml.LF.Ast
import           DA.Daml.StablePackages

entries :: [(Version, T.Text)]
entries = map toEntry allStablePackagesTuples
  where
    toEntry (id, pkg) = (packageLfVersion pkg, unPackageId id)

outputPath :: FilePath
outputPath = "compiler/damlc/stable-packages/json/stable-packages.json"

outputPath' :: FilePath
outputPath' = "compiler/damlc/stable-packages/json/stable-packages.yaml"

main :: IO ()
main = do
  putStrLn $ "Generating fresh json data for: " ++ outputPath
  LBS.writeFile outputPath (Aeson.encodePretty entries)
  BS.writeFile outputPath' (Yaml.encode entries)
  putStrLn "Successfully wrote dummy stable-packages.json."
