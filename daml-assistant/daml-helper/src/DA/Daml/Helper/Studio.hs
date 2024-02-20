-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Studio
    ( runDamlStudio
    , ReplaceExtension(..)
    ) where

import Control.Monad.Extra
import qualified Data.ByteString.Lazy.UTF8 as UTF8
import Data.Char (toLower)
import Data.Function (on)
import Data.Maybe
import System.Directory.Extra
import System.Environment hiding (setEnv)
import System.Exit
import System.FilePath
import System.Info.Extra
import System.IO.Extra
import System.Process (showCommandForUser)
import System.Process.Internals (translate)
import System.Process.Typed

import DA.Daml.Project.Consts

runDamlStudio :: ReplaceExtension -> [String] -> IO ()
runDamlStudio replaceExt remainingArguments = do
    sdkPath <- getSdkPath
    InstalledExtensions {..} <- getInstalledExtensions

    let bundledExtensionVsix = sdkPath </> "studio/daml-bundled.vsix"
        removeOldBundledExtension = whenJust oldBundled removePathForcibly
        uninstall name = do
            (exitCode, out, err) <- runVsCodeCommand ["--uninstall-extension", name]
            when (exitCode /= ExitSuccess) $ do
                hPutStrLn stderr . unlines $
                    [ "Failed to uninstall published version of Daml Studio."
                    , "Messages from VS Code:"
                    , "--- stdout ---"
                    , out
                    , "--- stderr ---"
                    , err
                    ]
                exitWith exitCode

        removeBundledExtension =
            when bundledInstalled $ uninstall bundledExtensionName

        removePublishedExtension =
            when publishedExtensionIsInstalled $ uninstall publishedExtensionName

        installBundledExtension' =
            installBundledExtension bundledExtensionVsix

        installPublishedExtension =
            when (not publishedExtensionIsInstalled) $ do
                (exitCode, _out, err) <- runVsCodeCommand
                    ["--install-extension", publishedExtensionName]
                when (exitCode /= ExitSuccess) $ do
                    hPutStr stderr . unlines $
                        [ err
                        , "Failed to install Daml Studio extension from marketplace."
                        , "Installing bundled Daml Studio extension instead."
                        ]
                    installBundledExtension'

    -- First, ensure extension is installed as requested.
    case replaceExt of
        ReplaceExtNever ->
            when (not bundledInstalled && isNothing oldBundled)
                installPublishedExtension

        ReplaceExtAlways -> do
            removePublishedExtension
            removeBundledExtension
            removeOldBundledExtension
            installBundledExtension'

        ReplaceExtPublished -> do
            removeBundledExtension
            removeOldBundledExtension
            installPublishedExtension

    -- Then, open visual studio code.
    projectPathM <- getProjectPath
    let path = fromMaybe "." projectPathM
    (exitCode, _out, err) <- runVsCodeCommand (path : remainingArguments)
    when (exitCode /= ExitSuccess) $ do
        hPutStrLn stderr . unlines $
            [ err
            , "Failed to launch Daml studio. Make sure Visual Studio Code is installed."
            , "See https://code.visualstudio.com/Download for installation instructions."
            ]
        exitWith exitCode

data ReplaceExtension
    = ReplaceExtNever
    -- ^ Never replace an existing extension.
    | ReplaceExtAlways
    -- ^ Always replace the extension.
    | ReplaceExtPublished
    -- ^ Replace with published extension (the default).

-- | Run VS Code command with arguments, returning the exit code, stdout & stderr.
runVsCodeCommand :: [String] -> IO (ExitCode, String, String)
runVsCodeCommand args = do
    originalEnv <- getEnvironment
    let strippedEnv = filter ((`notElem` damlEnvVars) . fst) originalEnv
            -- ^ Strip Daml environment variables before calling VSCode, to
            -- prevent setting DAML_SDK_VERSION too early. See issue #1666.
        commandEnv = addVsCodeToPath strippedEnv
            -- ^ Ensure "code" is in PATH before running command.
        command = toVsCodeCommand args
        process = setEnv commandEnv command
    (exit, out, err) <- readProcess process
    pure (exit, UTF8.toString out, UTF8.toString err)

-- | Add VSCode bin path to environment PATH. Only need to add it on Mac, as
-- VSCode is installed in PATH by default on the other platforms.
addVsCodeToPath :: [(String, String)] -> [(String,String)]
addVsCodeToPath env | isMac =
    let pathM = lookup "PATH" env
        newSearchPath = maybe "" (<> [searchPathSeparator]) pathM <>
            "/Applications/Visual Studio Code.app/Contents/Resources/app/bin"
    in ("PATH", newSearchPath) : filter ((/= "PATH") . fst) env
addVsCodeToPath env = env

-- | Directory where bundled extension gets installed.
getVsCodeExtensionsDir :: IO FilePath
getVsCodeExtensionsDir = fmap (</> ".vscode/extensions") getHomeDirectory

-- | Name of VS Code extension in the marketplace.
publishedExtensionName :: String
publishedExtensionName = "DigitalAssetHoldingsLLC.daml"

-- | Name of VS Code extension bundled with older SDKs (up to 0.13.12). Was
-- implemented as a symlink to extension files stored under ~/.daml.
oldBundledExtensionDirName :: String
oldBundledExtensionDirName = "da-vscode-daml-extension"

-- | Name of VS Code extension bundled with the SDK as a vsix.
bundledExtensionName :: String
bundledExtensionName = "DigitalAssetHoldingsLLC.daml-bundled"

-- | Status of installed VS Code extensions.
data InstalledExtensions = InstalledExtensions
    { oldBundled :: Maybe FilePath
        -- ^ bundled extension, if installed (0.13.12 and earlier)
    , bundledInstalled :: Bool
        -- ^ bundled extension, installed through vsix (0.13.13 and up)
    , publishedExtensionIsInstalled :: Bool
        -- ^ true if published extension is installed
    } deriving (Show, Eq)

-- | Get status of installed VS code extensions.
getInstalledExtensions :: IO InstalledExtensions
getInstalledExtensions = do
    oldBundled <- getOldExt
    extensions <- getExtensions
    let oldBundledIsInstalled = isJust oldBundled
        publishedExtensionIsInstalled =
            not oldBundledIsInstalled &&
                Lowercase publishedExtensionName `elem` extensions
        bundledInstalled = Lowercase bundledExtensionName `elem` extensions
    pure InstalledExtensions {..}
    where getOldExt :: IO (Maybe FilePath)
          getOldExt = do
              extensionsDir <- getVsCodeExtensionsDir
              let oldBundledDir = extensionsDir </> oldBundledExtensionDirName
              exists <- doesPathExist oldBundledDir
              pure $ if exists then Just oldBundledDir else Nothing

          getExtensions :: IO [Lowercase]
          getExtensions = do
              (_exitCode, extensionsStr, _err) <- runVsCodeCommand ["--list-extensions"]
              pure $ map Lowercase $ lines extensionsStr

newtype Lowercase = Lowercase { originalString :: String }
    deriving (Show)

instance Eq Lowercase where
    (==) = (==) `on` (map toLower . originalString)

instance Ord Lowercase where
    compare = compare `on` (map toLower . originalString)

installBundledExtension :: FilePath -> IO ()
installBundledExtension pathToVsix = do
    (exitCode, _out, err) <- runVsCodeCommand ["--install-extension", pathToVsix]
    when (exitCode /= ExitSuccess) $ do
        hPutStr stderr . unlines $
           [ err
           , "Failed to install Daml Studio extension from current SDK."
           , "Please open an issue on GitHub with the above message."
           , "https://github.com/digital-asset/daml/issues/new?template=bug_report.md"
           ]

-- [Note cmd.exe and why everything is horrible]
--
-- cmd.exe has a terrible behavior where it strips the first and last
-- quote under certain conditions, quoting cmd.exe /?:
--
-- > 1.  If all of the following conditions are met, then quote characters
-- >     on the command line are preserved:
-- >
-- >     - no /S switch
-- >     - exactly two quote characters
-- >     - no special characters between the two quote characters,
-- >       where special is one of: &<>()@^|
-- >     - there are one or more whitespace characters between the
-- >       the two quote characters
-- >     - the string between the two quote characters is the name
-- >       of an executable file.
-- >
-- > 2.  Otherwise, old behavior is to see if the first character is
-- >     a quote character and if so, strip the leading character and
-- >     remove the last quote character on the command line, preserving
-- >     any text after the last quote character.
--
-- By adding a quote at the beginning and the end, we always fall into 2
-- and the line after stripping the first and last quote corresponds to the one we want.
--
-- To make things worse that does not seem to be sufficient sadly:
-- If we quote `"code"` it seems to result in cmd.exe no longer looking
-- for it in PATH. I was unable to find any documentation or other information
-- on this behavior.

-- See [Note cmd.exe and why everything is horrible]
toVsCodeCommand :: [String] -> ProcessConfig () () ()
toVsCodeCommand args
    | isWindows = shell $ "\"" <> unwords ("code" : map translate args) <> "\""
    | otherwise = shell $ showCommandForUser "code" args

