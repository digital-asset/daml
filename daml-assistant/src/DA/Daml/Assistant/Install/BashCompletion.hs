
-- | Installation of bash completions. These are installed in
-- /usr/local/etc/bash_completion.d (on Mac) or /etc/bash_completion.d (on Linux).
-- These are not installed on Windows by default.
module DA.Daml.Assistant.Install.BashCompletion
    ( installBashCompletions
    ) where

import DA.Daml.Assistant.Types

import qualified Data.ByteString.Lazy as BSL
import Control.Monad.Extra (when, unlessM)
import System.Directory (doesDirectoryExist, doesFileExist)
import System.FilePath ((</>))
import System.Info.Extra (isMac, isWindows)
import System.Process.Typed (proc, readProcessStdout_)

installBashCompletions :: InstallOptions -> DamlPath -> (String -> IO ()) -> IO ()
installBashCompletions options damlPath output =
    when (shouldInstallBashCompletions options) $
        doInstallBashCompletions damlPath output

shouldInstallBashCompletions :: InstallOptions -> Bool
shouldInstallBashCompletions options =
    case iBashCompletions options of
        BashCompletions Yes -> True
        BashCompletions No -> False
        BashCompletions Auto -> not isWindows

doInstallBashCompletions :: DamlPath -> (String -> IO ()) -> IO ()
doInstallBashCompletions damlPath output = do
    dirExists <- doesDirectoryExist bashCompletionDir
    if dirExists
        then do
            unlessM (doesFileExist bashCompletionScript) $ do
                completionScript <- getCompletionScript damlPath
                BSL.writeFile bashCompletionScript completionScript
                output ("Bash completions for DAML assistant installed in " <> bashCompletionDir)
        else output ("Bash completions for DAML assistant not installed: " <> bashCompletionDir <> " does not exist")

getCompletionScript :: DamlPath -> IO BSL.ByteString
getCompletionScript damlPath = do
    let assistant = assistantPath damlPath
    readProcessStdout_ (proc assistant ["--bash-completion-script", assistant])

assistantPath :: DamlPath -> FilePath
assistantPath (DamlPath p) = p </> "bin" </> "daml"

bashCompletionDir :: FilePath
bashCompletionDir
    | isMac = "/usr/local/etc/bash_completion.d"
    | otherwise = "/etc/bash_completion.d"

bashCompletionScript :: FilePath
bashCompletionScript = bashCompletionDir </> "daml"
