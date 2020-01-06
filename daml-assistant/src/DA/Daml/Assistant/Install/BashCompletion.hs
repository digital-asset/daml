-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Installation of bash completions. The completion script is
-- installed in @~/.daml/bash_completion.sh@ and a hook is added
-- in @~/.bash_completion@ to invoke it if available.
module DA.Daml.Assistant.Install.BashCompletion
    ( installBashCompletions
    ) where

import DA.Daml.Assistant.Types

import qualified Data.ByteString.Lazy as BSL
import Control.Exception.Safe (tryIO, catchIO, displayException)
import Control.Monad.Extra (unless, andM, whenM)
import System.Directory (getHomeDirectory, getAppUserDataDirectory, doesFileExist, removePathForcibly)
import System.FilePath ((</>))
import System.Info.Extra (isWindows)
import System.Process.Typed (proc, readProcessStdout_)
import System.IO.Extra (readFileUTF8, writeFileUTF8)

-- | Install bash completion script if we should.
installBashCompletions :: InstallOptions -> DamlPath -> (String -> IO ()) -> IO ()
installBashCompletions options damlPath output =
    whenM (shouldInstallBashCompletions options damlPath) $
        doInstallBashCompletions damlPath output

-- | Should we install bash completions? By default, yes, but only if the
-- we're not on Windows and the completion script hasn't yet been generated.
shouldInstallBashCompletions :: InstallOptions -> DamlPath -> IO Bool
shouldInstallBashCompletions options damlPath =
    case iBashCompletions options of
        BashCompletions Yes -> pure True
        BashCompletions No -> pure False
        BashCompletions Auto -> andM
            [ pure (not isWindows)
            , not <$> doesFileExist (completionScriptPath damlPath)
            , isDefaultDamlPath damlPath
            ]

-- | Generate the bash completion script, and add a hook.
doInstallBashCompletions :: DamlPath -> (String -> IO ()) -> IO ()
doInstallBashCompletions damlPath output = do
    let scriptPath = completionScriptPath damlPath
    script <- getCompletionScript damlPath
    BSL.writeFile scriptPath script
    unitE <- tryIO $ addCompletionHook scriptPath
    case unitE of
        Left e -> do
            output ("Bash completions not installed: " <> displayException e)
            catchIO (removePathForcibly scriptPath) (const $ pure ())
        Right () -> output "Bash completions installed for DAML assistant."

-- | Read the bash completion script from optparse-applicative's
-- built-in @--bash-completion-script@ routine. Please read
-- https://github.com/pcapriotti/optparse-applicative/wiki/Bash-Completion
-- for more details. Note that the bash completion script doesn't
-- in general contain any daml-assistant specific information, it's only
-- specific to the path, so we don't need to regenerate it every version.
getCompletionScript :: DamlPath -> IO BSL.ByteString
getCompletionScript damlPath = do
    let assistant = assistantPath damlPath
    readProcessStdout_ (proc assistant ["--bash-completion-script", assistant])

-- | Add a completion hook in ~/.bash_completion
-- Does nothing if the hook is already there
addCompletionHook :: FilePath -> IO ()
addCompletionHook scriptPath = do
    let newHook = makeHook scriptPath
    hookPath <- getHookPath
    hooks <- readHooks hookPath
    unless (newHook `elem` hooks) $ do
        writeHooks hookPath (hooks ++ [newHook])

-- | Check the daml path is default. We don't want to install completions
-- for non-standard paths by default.
isDefaultDamlPath :: DamlPath -> IO Bool
isDefaultDamlPath (DamlPath damlPath) = do
    rawDamlPath <- tryIO (getAppUserDataDirectory "daml")
    pure $ Right damlPath == rawDamlPath

newtype HookPath = HookPath FilePath
newtype Hook = Hook { unHook :: String } deriving Eq

makeHook :: FilePath -> Hook
makeHook scriptPath = Hook $ concat
    [ "[ -f "
    , show scriptPath
    , " ] && source "
    , show scriptPath
    ]

getHookPath :: IO HookPath
getHookPath = do
    home <- getHomeDirectory
    pure (HookPath $ home </> ".bash_completion")

readHooks :: HookPath -> IO [Hook]
readHooks (HookPath p) = catchIO
    (map Hook . lines <$> readFileUTF8 p)
    (const $ pure [])

writeHooks :: HookPath -> [Hook] -> IO ()
writeHooks (HookPath p) = writeFileUTF8 p . unlines . map unHook

----

assistantPath :: DamlPath -> FilePath
assistantPath (DamlPath p) = p </> "bin" </> "daml"

completionScriptPath :: DamlPath -> FilePath
completionScriptPath (DamlPath p) = p </> "bash_completions.sh"
