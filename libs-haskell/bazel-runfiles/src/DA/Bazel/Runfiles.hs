-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- There is a library in rules_haskell that does something similar
-- but since we also want support for locating runfiles in JARs
-- it is simpler to have all code located here.
module DA.Bazel.Runfiles
  ( locateRunfiles
  , mainWorkspace
  ) where

import Control.Monad.Trans.Maybe
import Data.Foldable
import System.Directory
import System.Environment
import System.FilePath

mainWorkspace :: String
mainWorkspace = "com_github_digital_asset_daml"

locateRunfiles :: FilePath -> IO FilePath
locateRunfiles target = do
    execPath <- getExecutablePath
    mbDir <-runMaybeT . asum . map MaybeT $
        [ do let jarResources = takeDirectory execPath </> "resources"
             hasJarResources <- doesDirectoryExist jarResources
             pure $ if hasJarResources
                 then Just jarResources
                 else Nothing
        , do let runfilesDir = execPath <> ".runfiles"
             hasRunfiles <- doesDirectoryExist runfilesDir
             pure $ if hasRunfiles
                 then Just (runfilesDir </> target)
                 else Nothing
        , do mbDir <- lookupEnv "RUNFILES_DIR"
             pure (fmap (</> target) mbDir)
        ]
    maybe (error "Could not locate runfiles") pure mbDir
