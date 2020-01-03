-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Installation of bash completions. These are installed in
-- /usr/local/etc/bash_completion.d (on Mac) or
-- /etc/bash_completion.d (elsewhere). These are not installed on
-- Windows by default, and won't be reinstalled unless the user
-- asks by passing @--bash-completions=yes@ explicitly.
module DA.Daml.Assistant.Install.BashCompletion
    ( installBashCompletions
    ) where

import DA.Daml.Assistant.Types

import qualified Data.ByteString.Lazy as BSL
import Control.Monad.Extra (andM, whenM)
import System.Directory (doesDirectoryExist, doesFileExist)
import System.FilePath ((</>))
import System.Info.Extra (isMac, isWindows)
import System.Process.Typed (proc, readProcessStdout_)

installBashCompletions :: InstallOptions -> DamlPath -> (String -> IO ()) -> IO ()
installBashCompletions options damlPath output =
    whenM (shouldInstallBashCompletions options) $
        doInstallBashCompletions damlPath output

shouldInstallBashCompletions :: InstallOptions -> IO Bool
shouldInstallBashCompletions options =
    case iBashCompletions options of
        BashCompletions Yes -> pure True
        BashCompletions No -> pure False
        BashCompletions Auto -> andM
            [ pure (not isWindows)
            , doesDirectoryExist bashCompletionDirPath
            , not <$> doesFileExist bashCompletionScriptPath
            ]

doInstallBashCompletions :: DamlPath -> (String -> IO ()) -> IO ()
doInstallBashCompletions damlPath output = do
    dirExists <- doesDirectoryExist bashCompletionDirPath
    if dirExists
        then do
            completionScript <- getCompletionScript damlPath
            BSL.writeFile bashCompletionScriptPath completionScript
            output "Bash completions installed for DAML assistant."
        else output ("Bash completions not installed for DAML assistant: " <> bashCompletionDirPath <> " does not exist")

getCompletionScript :: DamlPath -> IO BSL.ByteString
getCompletionScript damlPath = do
    let assistant = assistantPath damlPath
    readProcessStdout_ (proc assistant ["--bash-completion-script", assistant])

assistantPath :: DamlPath -> FilePath
assistantPath (DamlPath p) = p </> "bin" </> "daml"

bashCompletionDirPath :: FilePath
bashCompletionDirPath
    | isMac = "/usr/local/etc/bash_completion.d"
    | otherwise = "/etc/bash_completion.d"

bashCompletionScriptPath :: FilePath
bashCompletionScriptPath = bashCompletionDirPath </> "daml"
