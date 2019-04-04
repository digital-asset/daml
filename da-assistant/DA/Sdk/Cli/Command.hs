-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Command
  ( getCommand
  , Command (..)
  , commandConfigProps
  , Action (..)
  , PrimitiveAction (..)
  , NormalAction (..)
  , TestAction (..)
  , alphaNumWithDashesParser
  , attributePairParser
  , destinationParser
  , qualifiedTemplateParser
  , namespacedTemplateParser
  , fetchArgP
  , groupByAttrName

  , docsHelp
  , changelogLink
  ) where

import           DA.Sdk.Prelude

import           Control.Monad.Logger        (LogLevel (..))
import qualified Data.Attoparsec.Text        as A
import           Data.Text                   (pack, unpack)
import           Options.Applicative

import           DA.Sdk.Cli.Command.Types
import           DA.Sdk.Cli.Conf             ( defaultLogLevel
                                             )
import           DA.Sdk.Cli.Repository.Types
import           DA.Sdk.Cli.Version          (version)
import qualified DA.Sdk.Version              as V
import qualified DA.Sdk.Cli.Template.Consts as Template
import Data.String

-- Execute the CLI parser on the argument string and fail or return a Command.
getCommand :: IO Command
getCommand = execParser cli

cli :: ParserInfo Command
cli = info (helper <*> commandParser)
  ( fullDesc
  <> progDesc "SDK Assistant. Use --help for help."
  <> header ("SDK Assistant - " <> unpack version) )

commandParser :: Parser Command
commandParser = Command
  <$> (fmap Just configFileParser <|> pure Nothing)
  <*> logLevelParser
  <*> isScriptParser
  <*> termWidthParser
  <*> actionParser

configFileParser :: Parser FilePath
configFileParser = option configFileReader
  ( long "config"
  <> short 'c'
  <> metavar "FILENAME"
  <> help "Specify what config file to use." )

logLevelParser :: Parser LogLevel
logLevelParser = option (attoReadM logLevelP)
  ( long "log-level"
  <> short 'l'
  <> metavar "debug|info|warn|error"
  <> value defaultLogLevel
  <> help "Set the log level. Default is 'warn'." )

isScriptParser :: Parser Bool
isScriptParser = switch
  ( long "script"
  <> help "Script mode -- skip auto-upgrades and similar." )

termWidthParser :: Parser (Maybe Int)
termWidthParser = option (optional auto)
  ( long "term-width"
  <> value Nothing
  <> help "Rendering width of the terminal." )

attoReadM :: A.Parser a -> ReadM a
attoReadM p = eitherReader (A.parseOnly p . pack)

logLevelP :: A.Parser LogLevel
logLevelP = A.choice
  [ A.string "debug" *> A.atEnd >> pure LevelDebug
  , A.string "info" *> A.atEnd >> pure LevelInfo
  , A.string "warn" *> A.atEnd >> pure LevelWarn
  , A.string "error" *> A.atEnd >> pure LevelError
  ]

configFileReader :: ReadM FilePath
configFileReader = eitherReader $ Right . textToPath . pack

actionParser :: Parser Action
actionParser = subparser
  (  subcommand "status" (pure $ Normal ShowStatus) "Show SDK environment overview"
  <> subcommand "new" (Normal <$> createProjectParser) "Create a new project from template"
  <> subcommand "add" (Normal <$> addToProjectParser) "Add a template to the current project"
  <> subcommand "project" (Normal <$> projectParser) "Manage DAML SDK projects"
  <> subcommand "template" (Normal <$> templateActionParser) "Manage DAML SDK templates"
  <> subcommand "upgrade" (pure $ Normal SdkUpgrade) "Upgrade to latest SDK version"
  <> subcommand "list" (pure $ Normal SdkList) "List installed SDK versions"
  <> subcommand "use" (Normal <$> sdkUseParser) "Set the default SDK version, downloading it if necessary"
  <> subcommand "uninstall" (Normal <$> sdkUninstallParser) "Remove DAML SDK versions or the complete DAML SDK"
  <> subcommand "start" (Normal . Start <$> servicesParser) "Start a given service"
  <> subcommand "restart" (Normal . Restart <$> servicesParser) "Restart a given service"
  <> subcommand "stop" (Normal . Stop <$> servicesParser) "Stop a given service"
  <> subcommand "feedback" (pure $ Normal SendFeedback) "Send us feedback!"
  <> subcommand "studio" (pure $ Normal Studio) "Start DAML Studio in the current project"
  <> subcommand "navigator" (pure $ Normal Navigator) "Start Navigator (also runs Sandbox if needed)"
  <> subcommand "sandbox" (pure $ Normal Sandbox) "Start Sandbox process in current project"
  <> subcommand "compile" (pure $ Normal Compile) "Compile DAML project into a DAR package"
  <> subcommand "path" (Normal <$> pathName) "Show the filesystem path of an SDK component"
  <> subcommand "run" (Normal <$> runCommand) "Run the main executable of a package"
  <> subcommand "setup" (pure $ Primitive DoSetup) "Set up SDK environment (e.g. on install or upgrade)"
  <> subcommand "subscribe" (Normal <$> (Subscribe <$> nameSpaceP)) "Subscribe for a namespace (of the template repository)."
  <> subcommand "unsubscribe" (Normal <$> (Unsubscribe <$> optional nameSpaceP)) "Unsubscribe from a namespace (of the template repository)."
  <> subcommand "config-help" (pure $ Primitive ShowConfigHelp) "Show config-file help"
  <> subcommand "config" (Normal <$> configParser) "Query and manage configuration"
  <> subcommand "update-info" (pure $ Normal DisplayUpdateChannelInfo) "Show SDK Assistant update channel information"
  <> subcommand "fetch" (Normal <$> fetchArgParser) "Fetch an SDK package"
  )

  -- Parse hidden commands
  <|> subparser (
        subcommand "test" (Primitive <$> testParser) "Test commands"
     <> subcommand "test-templates" (pure $ Normal TestTemplates) "Test templates in current active SDK version"
     <> subcommand "changelog" (Normal <$> changelog) "Show the changelog of an SDK version"
     <> subcommand "sdk-package" (Normal <$> sdk) "SDK maintenance commands."
     <> subcommand "use-experimental" (Normal <$> sdkUseExperimentalParser) "Set the default SDK version using the experimental repo"
     <> subcommand "docs" (pure $ Normal ShowDocs) docsHelp
     <> internal
  )

  <|> flag' (Primitive ShowVersion)
    ( short 'v'
    -- NOTE: The long --version flag is used by the install/upgrade script.
    <> long "version"
    <> help "Show version and exit"
    <> hidden )
  -- Default to showing plain status
  <|> pure (Normal ShowStatus)

createProjectParser :: Parser NormalAction
createProjectParser = aux <$> optional namespacedTemplate <*> optional projectPath
  where
    aux :: Maybe TemplateArg -> Maybe FilePath -> NormalAction
    aux  Nothing     _          = TemplateList TemplateListAsTable (Just Project)
    aux (Just path)  Nothing    = CreateProject Nothing (textToPath $ templateArgToText path)
    aux (Just name) (Just path) = CreateProject (Just name) path

    namespacedTemplate = argument (readTemplateArg Project)
                          ( metavar "PROJ_TEMPLATE" <> help ("Project template " <>
                              "(e.g.: <namespace>/<name>) If left out, default " <>
                              "template is used."))
    projectPath        = argument (fmap stringToPath str)
                          ( metavar "PROJECT_PATH" <> help
                          ("Path to the new project. Name of the last folder will "
                          <> "be the name of the new project." ))

addToProjectParser :: Parser NormalAction
addToProjectParser = aux <$> optional namespacedTemplate <*> optional targetFolder
  where
    aux :: Maybe TemplateArg -> Maybe FilePath -> NormalAction
    aux  Nothing    _     = TemplateList TemplateListAsTable (Just Addon)
    aux (Just arg) mbPath = TemplateAdd arg mbPath

    namespacedTemplate = argument (readTemplateArg Addon)
                          ( metavar "ADD_ON_TEMPLATE" <> help ("Add-on template " <>
                              "(e.g.: <namespace>/<name>) or project name." ))
    targetFolder       = argument (fmap stringToPath str)
                          ( metavar "TARGET_FOLDER" <> help
                          ("A folder under current project to copy the add-on "
                           <> "template into. Can be omitted if project name "
                           <> "is specified" ))

sdkUseParser :: Parser NormalAction
sdkUseParser = SdkUse <$> parseVersion

sdkUseExperimentalParser :: Parser NormalAction
sdkUseExperimentalParser = SdkUseExperimental <$> parseVersion

sdkUninstallParser :: Parser NormalAction
sdkUninstallParser = subparser
   (subcommand "all" (pure $ SdkUninstall UninstallAll) "Remove the complete DAML SDK")
   <|> SdkUninstall . Uninstall <$>  many parseVersion

configParser :: Parser NormalAction
configParser = subparser (
       subcommand "get" configGetParser "Get configuration properties"
    <> subcommand "set" configSetParser "Set a configuration property"
    )
  where
    configGetParser =
        ConfigGet <$>
          optional (
            argument (fmap pack str) (metavar "KEY" <> help "Configuration key")
          )
    configSetParser =
        ConfigSet
        <$> switch  ( long "project"
                <> short 'p'
                <> help "Whether to set the property in the project file or in the global one")
        <*> argument (fmap pack str) (metavar "KEY" <> help "Configuration key")
        <*> argument (fmap pack str) (metavar "VAL" <> help "Configuration value to be set")


servicesParser :: Parser Service
servicesParser =  subparser
  (subcommand "sandbox" (pure SandboxService) "DAML sandbox in current project"
  <>
  subcommand "navigator" (pure NavigatorServices) "DA navigator in current project"
  <>
  subcommand "all" (pure All) "All processes in current project")
  <|> pure All -- default starts everything

pathName :: Parser NormalAction
pathName =
    ShowPath
    <$> optional
          (argument
              (fmap pack str)
              (metavar "PACKAGE" <> help "Package Name"))
    <*> flag PPQExecutable PPQPackage
          ( long "package-path"
          <> help "show path to the package")

runCommand :: Parser NormalAction
runCommand =
    RunExec
    <$> optional
      ((,)
      <$> argument (fmap pack str) (metavar "PACKAGE" <> help "Package Name")
      <*> many
        (pack <$> strArgument (metavar "-- ARGS (e.g. da run damlc -- arg1 arg2)")))

fetchArgParser :: Parser NormalAction
fetchArgParser = FetchPackage <$> parseFetchArg <*> optional parseTarget <*> dontCheckFetchTag
  where
    parseFetchArg =
      argument
        (parseWith fetchArgP "fetch argument" "<package-name> or <prefix>/<package-name>)")
        (  metavar "FETCH_ARG"
        <> help "Fetch argument (<package-name> or <prefix>/<package-name>).")
    parseTarget =
      argument
        (fmap stringToPath str)
        (  metavar "TARGET"
        <> help "Target directory (for extraction).")
    dontCheckFetchTag =
      flag False True
        ( long "dont-check-fetch-tag"
        <> help "This flag switches off checking whether the tag 'fetch' is present."
        <> internal)

projectParser :: Parser NormalAction
projectParser = subparser
    (  subcommand "new" createProjectParser "Create a new project from template"
    <> subcommand "add" addToProjectParser "Add a template to the current project"
    )

templateActionParser :: Parser NormalAction
templateActionParser = subparser
    (  subcommand "publish" publishTemplateParser "Publish a new template to the configured repository"
    <> subcommand "list" (pure $ TemplateList TemplateListAsTuples Nothing) "List all available templates"
    <> subcommand "info" templateInfoParser "Get info about specific template"
    )

templateInfoParser :: Parser NormalAction
templateInfoParser = TemplateInfo <$> qualifiedTemplate
  where
    qualifiedTemplate = argument (parseWith qualifiedTemplateParser
                                            ("qualified template name.\nFormat: "
                                             <> qualifiedTId <> ",\nRelease-line format: "
                                             <> "<major>.<minor>.x or <major>.<minor>.<patch>.\n"
                                             <> "Error: ")
                                            ("Please use format " <> qualifiedTId))
                        ( metavar "QUALIFIED_TEMPLATE" <>
                          help ("A qualified template identifier like " <> qualifiedTId))
    qualifiedTId = "<namespace>/<name>/<release-line>"

publishTemplateParser :: Parser NormalAction
publishTemplateParser = TemplatePublish <$> qualifiedTemplate
  where
    qualifiedTemplate = argument (parseWith qualifiedTemplateParser
                                            ("qualified template name.\nFormat: "
                                             <> qualifiedTId <> ",\nRelease-line format: "
                                             <> "<major>.<minor>.x or <major>.<minor>.<patch>.\n"
                                             <> "Error: ")
                                             ("Please use format " <> qualifiedTId))
                        ( metavar "QUALIFIED_TEMPLATE" <>
                          help ("A qualified template identifier like " <> qualifiedTId))
    qualifiedTId = "<namespace>/<name>/<release-line>"

-- unsubscribeOrListParser :: Parser NormalAction
-- unsubscribeOrListParser = subscribeParser (Unsubscribe)
nameSpaceP :: Parser NameSpace
nameSpaceP = argument (parseWith (nameParser "Namespace" NameSpace) "namespace"
                        "Please use alphanumeric characters and dashes only, maximum 50 characters.")
                         ( metavar "NAMESPACE" <>
                           help "A namespace of the template repository")

groupByAttrName :: [(Text, Text)] -> [Attribute]
groupByAttrName attrNameValL = map (toAttr . unzip) groups
  where
    groups = groupBy (\(k1, _) (k2, _) -> k1 == k2) $ sort attrNameValL
    toAttr (names, vals) = Attribute (head names) (AVText vals)

attributePairParser :: A.Parser (Text, Text)
attributePairParser = do
    name  <- someDigitOrLetterP
    _sep  <- A.char '='
    val <- someDigitOrLetterP
    return (pack name, pack val)
  where
    someDigitOrLetterP = some $ A.digit <|> A.letter

destinationParser :: A.Parser (PackageName, GenericVersion)
destinationParser = do
    name        <- pkgNameP
    _sep        <- A.char '/'
    versionSpec <- versionSpecParser
    return (PackageName $ pack name, versionSpec)
  where
    pkgNameP     = some $ A.digit <|> A.letter <|> (A.satisfy $ A.inClass "-_:.+")

alphaNumWithDashesParser :: A.Parser Text
alphaNumWithDashesParser = do
    c <- A.digit <|> A.letter
    cs <- many $ A.digit <|> A.letter <|> A.char '-'
    return $ pack (c : cs)

templateArgParser :: TemplateType -> A.Parser TemplateArg
templateArgParser ty = uncurry Qualified <$> namespacedTemplateParser
                   <|> BuiltIn <$> builtInTemplateParser ty

qualifiedTemplateParser :: A.Parser (NameSpace, TemplateName, ReleaseLine)
qualifiedTemplateParser = do
    nameSpace   <- nameParser "Namespace" NameSpace
    _sep1       <- A.char '/'
    templName   <- nameParser "Template name" TemplateName
    _sep2       <- A.char '/'
    releaseLine <- releaseLineParser
    return (nameSpace, templName, releaseLine)

builtInTemplateParser :: TemplateType -> A.Parser Text
builtInTemplateParser ty = do
    x <- nameParser "Built-in template" id
    when (x `notElem` builtInList ty) $
      fail . unpack $ x <> " is not a built-in " <> tytxt ty <> " template."
    return x
  where
    builtInList Project = Template.builtInProjectTemplateNames
    builtInList Addon   = Template.builtInAddonTemplateNames
    tytxt Project = "project"
    tytxt Addon   = "add-on"

namespacedTemplateParser :: A.Parser (NameSpace, TemplateName)
namespacedTemplateParser = do
    nameSpace   <- nameParser "Namespace" NameSpace
    _sep1       <- A.char '/'
    templName   <- nameParser "Template name" TemplateName
    return (nameSpace, templName)

-- A namespace name MUST match the [a-z][-a-z0-9]{1,49} regular expression.
-- A template name MUST match the [a-z][-a-z0-9]{1,49} regular expression.
nameParser :: String -> (Text -> a) -> A.Parser a
nameParser name ctor = do
    firstChar <- A.satisfy $ A.inClass "a-z"
    others    <- some $ A.satisfy $ A.inClass "-a-z0-9"
    when (length  others > 49) $ fail (name <> "should not be longer than 50 characters.")
    return $ ctor $ pack (firstChar : others)

fetchArgP :: A.Parser FetchArg
fetchArgP = do
    fetchPrefix <- optional $ do
      prefix <-someFetchArgChars
      _sep1 <- A.char '/'
      return prefix
    fetchName   <- someFetchArgChars
    return $ FetchArg (FetchPrefix . pack <$> fetchPrefix) $ FetchName $ pack fetchName
  where
    someFetchArgChars = some $ A.satisfy $ A.inClass "-A-Za-z0-9._"

releaseLineParser :: A.Parser ReleaseLine
releaseLineParser = do
  major <- A.decimal :: A.Parser Int
  _sep1 <- A.char '.'
  minor <- A.decimal :: A.Parser Int
  _sep2 <- A.char '.'
  lastP <- (Left <$> A.char 'x') <|> (Right <$> A.decimal :: A.Parser (Either Char Int))
  return $ case lastP of
    Left _x     -> ReleaseLine major minor
    Right patch -> ExactRelease major minor patch

readTemplateArg :: TemplateType -> ReadM TemplateArg
readTemplateArg ty = do
  s <- str
  return . either (const (Malformed s)) id
         . A.parseOnly (templateArgParser ty <* A.endOfInput)
         $ s

parseWith :: A.Parser b -> String -> String -> ReadM b
parseWith parser what allowedFormat = do
    s <- str
    case A.parseOnly (parser <* A.endOfInput) s of
      Left "endOfInput" -> readerErr ("allowed format: " <> allowedFormat)
      Left err          -> readerErr ("Error: " <> err)
      Right r           -> return r
  where
    readerErr e = readerError ("Cannot parse " <> what <> ". " <> e)

changelog :: Parser NormalAction
changelog = Changelog <$> optional parseVersion

testParser :: Parser PrimitiveAction
testParser = Test <$> subparser
    (  subcommand "parse"
        (ParseSDKYaml <$> fileName)
        "Parse the sdk.yaml file"
    <> subcommand "download-docs"
        (DownloadDocs <$> user <*> apiKey <*> fileName <*> outPath)
        "Download documentation specified in sdk.yaml"
    )
  where
    user = argument (fmap pack str) (metavar "USER" <> help "Username")
    apiKey = argument (fmap pack str) (metavar "KEY" <> help "API Key")
    fileName = argument configFileReader (metavar "FILENAME" <> help "Filename")
    outPath = argument configFileReader (metavar "OUT" <> help "Output path")

sdk :: Parser NormalAction
sdk = Sdk <$> subparser
    ( subcommand
        "tags"
        sdkTags
        "Edit and list tags of an SDK release."
    )
  where
    sdkTags = SdkActionTags <$> subparser
        ( subcommand
            "list"
            (SdkTagsActionList <$> sdkActionList)
            "List tags of an SDK release."
        <> subcommand
            "add"
            (sdkActionVersionWithTags SdkTagsActionAdd)
            "Add tags to an SDK release."
        <> subcommand
            "remove"
            (sdkActionVersionWithTags SdkTagsActionRemove)
            "Remove tags from an SDK release."
        <> subcommand
            "sync"
            (SdkTagsActionSync <$> fileName)
            "Synchronize tags from a file to bintray."
        )

    sdkActionList :: Parser (Maybe V.SemVersion)
    sdkActionList = optional parseVersion

    sdkActionVersionWithTags ::
           (V.SemVersion -> [Text] -> SdkTagsAction)
        -> Parser SdkTagsAction
    sdkActionVersionWithTags constructor =
        constructor <$> parseVersion <*> tags

    tags :: Parser [Text]
    tags = many
          ( pack <$> strArgument (metavar "TAG")
          )

    fileName = argument configFileReader (metavar "FILENAME" <> help "Filename")

subcommand :: String -> Parser a -> String -> Mod CommandFields a
subcommand cmd parser desc = command cmd (info parser' infomod)
  where
    parser' = helper <*> parser
    infomod = progDesc desc

parseVersion :: Parser V.SemVersion
parseVersion = argument parser (metavar "VERSION" <> help "SDK version of form x.y.z")
  where
    parser = do
        s <- str
        case V.parseSemVersion (pack s) of
          Nothing  -> readerError ("Cannot parse semantic version: " <> s)
          Just ver -> return ver

docsHelp :: IsString a => a
docsHelp = "The DAML SDK documentation is no longer available offline. \
           \Please go to https://docs.daml.com to read the latest documentation."


changelogLink :: IsString a => a
changelogLink = "The  change log is no longer avilable offline. \
                 \ Please go to https://docs.daml.com/support/release-notes.html"
