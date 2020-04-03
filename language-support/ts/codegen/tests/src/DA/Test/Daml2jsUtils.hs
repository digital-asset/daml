-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Test.Daml2jsUtils (writeRootPackageJson) where

import qualified Data.Text.Extended as T
import qualified Data.ByteString.Lazy as BSL
import qualified Data.HashMap.Strict as HMS
import Data.Aeson
import System.FilePath

-- The need for this utility comes up in at least the assistant
-- integration and daml2js tests.
writeRootPackageJson :: Maybe FilePath -> [String] -> IO ()
writeRootPackageJson dir workspaces =
  BSL.writeFile (maybe "package.json" (</> "package.json") dir) $ encode $
  object
  [ "private" .= True
  , "workspaces" .= map T.pack workspaces
  , "resolutions" .= HMS.fromList ([ ("@daml/types", "file:daml-types") ] :: [(T.Text, T.Text)])
  ]
