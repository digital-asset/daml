-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Codegen
  ( runCodegen
  , Lang(..)
  , showLang
  ) where

import Control.Exception
import Control.Exception.Safe (catchIO)
import DA.Daml.Helper.Util
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import qualified Data.Text as T
import System.FilePath
import System.IO.Extra
import System.Process.Typed

data Lang
  = Java
  | JavaScript
  deriving (Show, Eq, Ord, Enum, Bounded)

showLang :: Lang -> T.Text
showLang = \case
  Java -> "java"
  JavaScript -> "js"

runCodegen :: Lang -> [String] -> IO ()
runCodegen lang args =
  case lang of
    JavaScript -> do
      args' <-
        if null args
          then do
            darPath <- getDarPath
            projectConfig <- getProjectConfig Nothing
            outputPath <-
              requiredE
                "Failed to read output directory for JavaScript code generation" $
              queryProjectConfigRequired
                ["codegen", showLang lang, "output-directory"]
                projectConfig
            mbNpmScope :: Maybe FilePath <-
              requiredE "Failed to read NPM scope for JavaScript code generation" $
              queryProjectConfig
                ["codegen", showLang lang, "npm-scope"]
                projectConfig
            pure $
              [darPath, "-o", outputPath] ++
              ["-s" <> npmScope | Just npmScope <- [mbNpmScope]]
          else pure args
      daml2js <- fmap (</> "daml2js" </> "daml2js") getSdkPath
      withProcessWait_' (proc daml2js args') (const $ pure ()) `catchIO`
        (\e -> hPutStrLn stderr "Failed to invoke daml2js." *> throwIO e)
    Java ->
      runJar
        "daml-sdk/daml-sdk.jar"
        (Just "daml-sdk/codegen-logback.xml")
        (["codegen", "java"] ++ args)
