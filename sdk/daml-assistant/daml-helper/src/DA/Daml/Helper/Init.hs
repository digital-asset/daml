-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Init
    ( runInit
    ) where

import Control.Monad.Extra
import qualified Data.ByteString as BS
import Data.List
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Yaml as Y
import qualified Data.Yaml.Pretty as Y
import System.Directory.Extra
import System.Exit
import System.FilePath
import System.IO
import System.Process (showCommandForUser)

import DA.Daml.Helper.Util
import DA.Daml.Project.Consts

-- | Initialize a daml package in the current or specified directory.
-- It will do the following (first that applies):
--
-- 1. If the target folder is actually a file, it will error out.
--
-- 2. If the target folder does not exist, it will error out and ask
-- the user if they meant to use daml new instead.
--
-- 3. If the target folder is a daml package root, it will do nothing
-- and let the user know the target is already a daml package.
--
-- 4. If the target folder is inside a daml package (transitively) but
-- is not the package root, it will do nothing and print out a warning.
--
-- 5. If none of the above, it will create a daml.yaml from scratch.
-- It will attempt to find a Main.daml source file in the package
-- directory tree, but if it does not it will use daml/Main.daml
-- as the default.
--
runInit :: Maybe FilePath -> IO ()
runInit targetFolderM = do
    currentDir <- getCurrentDirectory
    let targetFolder = fromMaybe currentDir targetFolderM
        targetFolderRel = makeRelative currentDir targetFolder
        packageConfigRel = normalise (targetFolderRel </> packageConfigName)
          -- ^ for display purposes

    -- cases 1 or 2
    unlessM (doesDirectoryExist targetFolder) $ do
        whenM (doesFileExist targetFolder) $ do
            hPutStr stderr $ unlines
                [ "ERROR: daml init target should be a directory, but is a file."
                , "    target = " <> targetFolderRel
                ]
            exitFailure

        hPutStr stderr $ unlines
            [ "ERROR: daml init target does not exist."
            , "    target = " <> targetFolderRel
            , ""
            , "To create a package directory use daml new instead:"
            , "    " <> showCommandForUser "daml" ["new", targetFolderRel]
            ]
        exitFailure
    targetFolderAbs <- makeAbsolute targetFolder -- necessary to find package roots

    -- cases 3 or 4
    mPackagePath <- findDamlPackageRoot targetFolderAbs
    whenJust mPackagePath $ \packageRoot -> do
        let relPackagePath = makeRelative currentDir packageRoot
        hPutStrLn stderr $ "Daml package already initialized at " <> relPackagePath
        when (targetFolderAbs /= packageRoot) $ do
            hPutStr stderr $ unlines
                [ "WARNING: daml init target is not the Daml package root."
                , "    daml init target  = " <> targetFolder
                , "    Daml package root = " <> relPackagePath
                ]
        exitSuccess

    -- case 5
    putStrLn ("Generating " <> packageConfigRel)

    currentSdkVersion <- getSdkVersion

    packageFiles <- listFilesRecursive targetFolder
    let targetFolderSep = addTrailingPathSeparator targetFolder
    let relPackageFiles = mapMaybe (stripPrefix targetFolderSep) packageFiles
    let isMainDotDaml = (== "Main.daml") . takeFileName
        sourceM = find isMainDotDaml relPackageFiles
        source = fromMaybe "daml/Main.daml" sourceM
        name = takeFileName (dropTrailingPathSeparator targetFolderAbs)

    BS.writeFile (targetFolder </> packageConfigName) . Y.encodePretty yamlConfig $ Y.object
        [ ("sdk-version", Y.String (T.pack currentSdkVersion))
        , ("name", Y.String (T.pack name))
        , ("source", Y.String (T.pack source))
        , ("scenario", Y.String "Main:mainScenario")
        , ("parties", Y.array [Y.String "Alice", Y.String "Bob"])
        , ("version", Y.String "1.0.0")
        , ("dependencies", Y.array [Y.String "daml-prim", Y.String "daml-stdlib"])
        ]

    putStr $ unlines
        [ "Initialized package " <> name
        , "Done! Please verify " <> packageConfigRel
        ]

    where

        fieldOrder :: [T.Text]
        fieldOrder =
            [ "sdk-version"
            , "name"
            , "version"
            , "source"
            , "scenario"
            , "parties"
            , "exposed-modules"
            , "dependencies"
            ]

        fieldNameCompare :: T.Text -> T.Text -> Ordering
        fieldNameCompare a b = compare (elemIndex a fieldOrder) (elemIndex b fieldOrder)

        yamlConfig :: Y.Config
        yamlConfig = Y.setConfCompare fieldNameCompare Y.defConfig

