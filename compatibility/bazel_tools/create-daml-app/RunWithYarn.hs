-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Bazel.Runfiles qualified
import Data.List
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import System.Process

main :: IO ()
main = withTempDir $ \yarnCache -> do
    setEnv "YARN_CACHE_FOLDER" yarnCache True
    (dir : args) <- getArgs
    runfiles <- Bazel.Runfiles.create
    let yarn | isWindows = Bazel.Runfiles.rlocation runfiles $ "nodejs_windows_amd64" </> "bin" </> "yarn.cmd"
          | isMac = Bazel.Runfiles.rlocation runfiles $ "nodejs_darwin_amd64" </> "bin" </> "yarn"
          | otherwise = Bazel.Runfiles.rlocation runfiles $ "nodejs_linux_amd64" </> "bin" </> "yarn"
    sp <- getSearchPath
    -- For reasons unbeknown to me setting PATH in the Bash script results in
    -- yarn pointing to hash/execroot/compatibility/external/nodejs_windows_amd64/bin/yarn.cmd.
    -- While this file exists just fine, this results in the error
    -- `The system cannot find the path specified.` for reasons nobody understands.
    -- Adding it to the runfiles of this binary and locating it here
    -- points to hash/external/nodejs_windows_amd64/bin/yarn.cmd.
    -- This works just fine.
    setEnv "PATH" (intercalate [searchPathSeparator] (takeDirectory yarn : sp)) True
    -- We write out the yarn workspace file here since it makes it much easier to
    -- get the paths right on Windows.
    writeFileUTF8 (dir </> "package.json") $ unlines
      [ "{"
      , "  \"private\": true,"
      , "  \"workspaces\": [\"daml.js\"],"
      , "  \"resolutions\": {"
      , "     \"@daml/types\": " <> show ("file:" <> dir  </> "daml-types") <> ","
      , "     \"@daml/ledger\": " <> show ("file:" <> dir  </> "daml-ledger")
      , "  }"
      , "}"
      ]
    callCommand (unwords args)

