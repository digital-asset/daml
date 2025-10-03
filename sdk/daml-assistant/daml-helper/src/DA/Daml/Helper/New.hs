-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.New
    ( runListTemplates
    , runNew
    , defaultProjectTemplate
    , ociMain
    ) where

import Control.Monad.Extra
import DA.Directory
import Data.Foldable
import Data.List.Extra
import Data.Maybe
import Options.Applicative
import System.Directory.Extra
import System.Environment (withProgName)
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process (showCommandForUser)

import DA.Bazel.Runfiles
import DA.Daml.Project.Consts
import DA.Daml.Project.Util
import DA.Signals (installSignalHandlers)

-- | Create a Daml package or project in a new directory, based on a template packaged
-- with the SDK. Special care has been taken to avoid:
--
-- * Project name/template name confusion: i.e. when a user passes a
-- single argument, it should be the new project/package folder. But if the user
-- passes an existing template name instead, we ask the user to be more
-- explicit.
-- * Creation of a project/package in existing folder (suggest daml init instead).
-- * Creation of a package inside a project.
--
runNew :: FilePath -> Maybe String -> IO ()
runNew targetFolder templateNameM = do
  sdkVersion <- getSdkVersion
  runNewInternal "daml" sdkVersion False targetFolder templateNameM

-- | Called for both Daml Assistant and DPM
runNewInternal :: String -> String -> Bool -> FilePath -> Maybe String -> IO ()
runNewInternal assistantName sdkVersion allowExisting targetFolder templateNameM = do
    templatesFolder <- getTemplatesFolder
    let templateName = fromMaybe defaultProjectTemplate templateNameM
        templateFolder = templatesFolder </> templateName
        projectName = takeFileName (dropTrailingPathSeparator targetFolder)

    -- Ensure template exists.
    unlessM (doesDirectoryExist templateFolder) $ do
        hPutStr stderr $ unlines
            [ "Template " <> show templateName <> " does not exist."
            , "Use `" <> assistantName <> " new --list` to see a list of available templates"
            ]
        exitFailure

    -- Ensure project directory does not already exist.
    whenM (doesDirectoryExist targetFolder) $ do
        unless allowExisting $ do
            hPutStr stderr $ unlines
                [ "Directory " <> show targetFolder <> " already exists."
                , "Please specify a new directory, or use '" <> assistantName <> " init' instead:"
                , ""
                , "    " <> showCommandForUser assistantName ["init", targetFolder]
                , ""
                ]
            exitFailure

    -- Ensure user is not confusing template name with project name.
    --
    -- We check projectName == targetFolder because if the user
    -- gave a targetFolder that isn't a straight up file name (it
    -- contains path separators), then it's likely that they did
    -- intend to pass a target folder and not a template name.
    when (isNothing templateNameM && projectName == targetFolder) $ do
        whenM (doesDirectoryExist (templatesFolder </> projectName)) $ do
            hPutStr stderr $ unlines
                [ "Template name " <> projectName <> " was given as project name."
                , "Please specify a project name separately, for example:"
                , ""
                , "    " <> showCommandForUser assistantName ["new", "myproject", "--template", projectName]
                , ""
                ]
            exitFailure

    -- Ensure we are not creating a project inside another project.
    targetFolderAbs <- makeAbsolute targetFolder

    damlRootM <- findDamlPackageRoot targetFolderAbs
    whenJust damlRootM $ \damlRoot -> do
        hPutStr stderr $ unlines
            [ "Target directory is inside existing Daml project " <> show damlRoot
            , "Please specify a new directory outside an existing project."
            ]
        exitFailure

    -- Copy the template over.
    copyDirectory templateFolder targetFolder
    files <- listFilesRecursive targetFolder
    mapM_ setWritable files

    -- Substitute strings in template files (not a Daml template!)
    -- e.g. the SDK version numbers in daml.yaml and package.json
    let templateFiles = filter (".template" `isExtensionOf`) files
    forM_ templateFiles $ \templateFile -> do
        templateContent <- readFileUTF8 templateFile
        let content = replace "__VERSION__"  sdkVersion
                    . replace "__PROJECT_NAME__" projectName
                    $ templateContent
            realFile = dropExtension templateFile
        writeFileUTF8 realFile content
        removeFile templateFile

    -- Done.
    putStrLn $
        "Created a new project in \"" <> targetFolder <>
        "\" based on the template \"" <> templateName <> "\"."

getTemplatesFolder :: IO FilePath
getTemplatesFolder = do
  mSdkPath <- tryGetSdkPath
  case mSdkPath of
    Just sdkPath -> pure $ sdkPath </> "templates"
    Nothing ->
      -- Templates are stored at root in resources, so locate empty resource to get root resources directory
      locateResource $ Resource "" ""

defaultProjectTemplate :: String
defaultProjectTemplate = "skeleton"

runListTemplates :: IO ()
runListTemplates = do
    templatesFolder <- getTemplatesFolder
    templates <- listDirectory templatesFolder
    if null templates
       then putStrLn "No templates are available."
       else putStrLn $ unlines $
          "The following templates are available:" :
          (map ("  " <>) . sort . map takeFileName) templates

-- | Our SDK installation is read-only to prevent users from accidentally modifying it.
-- But when we copy from it in "daml new" we want the result to be writable.
setWritable :: FilePath -> IO ()
setWritable f = do
    p <- getPermissions f
    setPermissions f p { writable = True }

-- Copied from Helper.Util to avoid its much larger dependency set. To be cleaned up once Daml Helper is removed.
findDamlPackageRoot :: FilePath -> IO (Maybe FilePath)
findDamlPackageRoot = findAscendantWithFile packageConfigName

findAscendantWithFile :: FilePath -> FilePath -> IO (Maybe FilePath)
findAscendantWithFile filename path =
    findM (\p -> doesFileExist (p </> filename)) (ascendants path)

ociMain :: IO ()
ociMain = withProgName "dpm" $ do
    forM_ [stdout, stderr] $ \h -> hSetBuffering h LineBuffering
    -- Save the runfiles environment to work around
    -- https://gitlab.haskell.org/ghc/ghc/-/issues/18418.
    setRunfilesEnv
    installSignalHandlers
    join $ customExecParser (prefs showHelpOnError) (info (commandParser <**> helper) idm)
  where
    commandParser :: Parser (IO ())
    commandParser = 
      let templateHelpStr = "Name of the template used to create the project (default: " <> defaultProjectTemplate <> ")"
          appTemplateFlag = asum
            [ Just <$> strOption (long "template" <> metavar "TEMPLATE" <> help templateHelpStr)
            , pure Nothing
            ]
          runNewDpm :: Bool -> Maybe FilePath -> Maybe String -> IO ()
          runNewDpm allowExisting oTargetFolder templateNameM = do
            sdkVersion <- getSdkVersionDpm
            runNewInternal "dpm" sdkVersion allowExisting (fromMaybe "." oTargetFolder) templateNameM
          commandNewParser :: Parser (IO ())
          commandNewParser = asum
            [ runListTemplates <$ flag' () (long "list" <> help "List the available project templates.")
            , runNewDpm False
                <$> (Just <$> argument str (metavar "TARGET_PATH" <> help "Path where the new project should be located"))
                <*> appTemplateFlag
            ]
          commandInitParser :: Parser (IO ())
          commandInitParser =
            runNewDpm True
              <$> optional (argument str (metavar "TARGET_PATH" <> help "Path where the new project should be located"))
              <*> pure (Just "empty-skeleton")
      in subparser $ fold 
        [ command "new" (info (commandNewParser <**> helper) idm)
        , command "init" (info (commandInitParser <**> helper) idm)
        ]
        
