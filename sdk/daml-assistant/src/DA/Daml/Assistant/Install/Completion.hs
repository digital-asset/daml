-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Installation of bash and Zsh completions. The Zsh completion script is
-- installed in @~/.daml/bash_completion.sh@ and a hook is added
-- in @~/.bash_completion@ to invoke it if available.
-- For Zsh we install the script to @~/.daml/zsh/_daml@ and ask users to add
-- @~/.daml/zsh@ to @$fpath@.
module DA.Daml.Assistant.Install.Completion
    ( installBashCompletions
    , installZshCompletions
    ) where

import DA.Daml.Assistant.Types

import qualified Data.ByteString.Lazy as BSL
import Control.Exception.Safe (tryIO, catchIO, displayException)
import Control.Monad.Extra (unless, andM, whenM)
import System.Directory (getHomeDirectory, getAppUserDataDirectory, doesFileExist, removePathForcibly, createDirectoryIfMissing)
import System.FilePath ((</>), takeDirectory)
import System.Info.Extra (isWindows)
import System.Process.Typed (proc, readProcessStdout_)
import System.IO.Extra (readFileUTF8, writeFileUTF8)

-- | Install bash completion script if we should.
installBashCompletions :: InstallOptions -> DamlPath -> (String -> IO ()) -> IO ()
installBashCompletions options damlPath output =
    whenM (shouldInstallBashCompletions options damlPath) $
        doInstallBashCompletions damlPath output

-- | Install zsh completion script if we should.
installZshCompletions :: InstallOptions -> DamlPath -> (String -> IO ()) -> IO ()
installZshCompletions options damlPath output =
    whenM (shouldInstallZshCompletions options damlPath) $
        doInstallZshCompletions damlPath output

-- | Should we install bash completions? By default, yes, but only if the
-- we're not on Windows and the completion script hasn't yet been generated.
shouldInstallBashCompletions :: InstallOptions -> DamlPath -> IO Bool
shouldInstallBashCompletions options damlPath =
    case iBashCompletions options of
        BashCompletions Yes -> pure True
        BashCompletions No -> pure False
        BashCompletions Auto -> andM
            [ pure (not isWindows)
            , not <$> doesFileExist (bashCompletionScriptPath damlPath)
            , isDefaultDamlPath damlPath
            ]

-- | Should we install Zsh completions? By default, yes, but only if the
-- we're not on Windows and the completion script hasn't yet been generated.
shouldInstallZshCompletions :: InstallOptions -> DamlPath -> IO Bool
shouldInstallZshCompletions options damlPath =
    case iZshCompletions options of
        ZshCompletions Yes -> pure True
        ZshCompletions No -> pure False
        ZshCompletions Auto -> andM
            [ pure (not isWindows)
            , not <$> doesFileExist (zshCompletionScriptPath damlPath)
            , isDefaultDamlPath damlPath
            ]

-- | Generate the bash completion script, and add a hook.
doInstallBashCompletions :: DamlPath -> (String -> IO ()) -> IO ()
doInstallBashCompletions damlPath output = do
    let scriptPath = bashCompletionScriptPath damlPath
    script <- getBashCompletionScript damlPath
    BSL.writeFile scriptPath script
    unitE <- tryIO $ addBashCompletionHook scriptPath
    case unitE of
        Left e -> do
            output ("Bash completions not installed: " <> displayException e)
            catchIO (removePathForcibly scriptPath) (const $ pure ())
        Right () -> output "Bash completions installed for Daml assistant."

-- | Generate the Zsh completion script.
doInstallZshCompletions :: DamlPath -> (String -> IO ()) -> IO ()
doInstallZshCompletions damlPath output = do
    let scriptPath = zshCompletionScriptPath damlPath
    script <- getZshCompletionScript damlPath
    createDirectoryIfMissing True (takeDirectory scriptPath)
    BSL.writeFile scriptPath script
    output $ unlines
        [ "Zsh completions installed for Daml assistant."
        , "To use them, add '~/.daml/zsh' to your $fpath, e.g. by adding the following"
        , "to the beginning of '~/.zshrc' before you call 'compinit':"
        , "fpath=(~/.daml/zsh $fpath)"
        ]

-- | Read the bash completion script from optparse-applicative's
-- built-in @--bash-completion-script@ routine. Please read
-- https://github.com/pcapriotti/optparse-applicative/wiki/Bash-Completion
-- for more details. Note that the bash completion script doesn't
-- in general contain any daml-assistant specific information, it's only
-- specific to the path, so we don't need to regenerate it every version.
getBashCompletionScript :: DamlPath -> IO BSL.ByteString
getBashCompletionScript damlPath = do
    let assistant = assistantPath damlPath
    readProcessStdout_ (proc assistant ["--bash-completion-script", assistant])

-- | Read the Zsh completion script from optparse-applicative’s
-- builtin @--zsh-completion-script@ routine.
getZshCompletionScript :: DamlPath -> IO BSL.ByteString
getZshCompletionScript damlPath = do
    let assistant = assistantPath damlPath
    readProcessStdout_ (proc assistant ["--zsh-completion-script", assistant])

-- | Add a completion hook in ~/.bash_completion
-- Does nothing if the hook is already there
addBashCompletionHook :: FilePath -> IO ()
addBashCompletionHook scriptPath = do
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

bashCompletionScriptPath :: DamlPath -> FilePath
bashCompletionScriptPath (DamlPath p) = p </> "bash_completions.sh"

zshCompletionScriptPath :: DamlPath -> FilePath
zshCompletionScriptPath (DamlPath p) = p </> "zsh" </> "_daml"
