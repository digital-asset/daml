-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Command.MultiIde.DocDependencies (unpackDoc, packageDbAndLfFromPath, anchorToTryGetDefinitionName, GetDocDefinition (..)) where

import Control.Monad (guard, unless)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT)
import DA.Cli.Damlc.Command.MultiIde.Types (MultiIdeState (..))
import DA.Cli.Damlc.Packaging (baseImports, getDepImports, getExposedModules)
import DA.Daml.Compiler.Dar (getDamlRootFiles)
import DA.Daml.Compiler.Output (diagnosticsLogger)
import DA.Daml.Doc.Driver (runDamlDoc, DamldocOptions (..), InputFormat (..), OutputFormat (..), ExternalAnchorBehaviour (..))
import DA.Daml.Doc.Extract (ExtractOptions (..), defaultExtractOptions)
import DA.Daml.Doc.Render.Types (RenderFormat (..))
import DA.Daml.Doc.Transform (defaultTransformOptions)
import DA.Daml.Doc.Types (AnchorGenerators (..), Anchor (..), Typename (..), Fieldname (..), Modulename (..), Packagename (..))
import DA.Daml.LanguageServer.SplitGotoDefinition (TryGetDefinitionName (..), TryGetDefinitionNameSpace (..))
import DA.Daml.LF.Ast.Version (Version, renderVersion, parseVersion)
import DA.Daml.Options.Types (Options (..), IgnorePackageMetadata (..), defaultOptions)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy.UTF8 as BSLU
import Data.List.Extra (dropEnd, find, isPrefixOf)
import qualified Data.Map as Map
import Data.List.Split (splitWhen)
import qualified Data.Text as T
import Development.IDE.Core.Rules.Daml (generatePackageMap)
import qualified Language.LSP.Types as LSP
import qualified Network.URI as URI
import SdkVersion.Class (SdkVersioned)
import System.Directory (createDirectoryIfMissing, doesFileExist, removePathForcibly, createDirectory, getDirectoryContents)
import System.FilePath (splitDirectories, joinPath, (</>))

packageDbAndLfFromPath :: FilePath -> TryGetDefinitionName -> Either String (FilePath, Version)
packageDbAndLfFromPath modulePath name = do
  let packageRootDirList = dropEnd (length $ splitWhen (=='.') $ tgdnModuleName name) $ splitDirectories modulePath
  lfVersion <- maybe (Left "Failed to parse LF Version in package database") Right $ parseVersion $ last $ init packageRootDirList
  let packageDb = joinPath $ dropEnd 2 packageRootDirList
  pure (packageDb, lfVersion)

hoistMaybe :: Applicative m => Maybe b -> MaybeT m b
hoistMaybe = MaybeT . pure

unpackDoc :: SdkVersioned => MultiIdeState -> FilePath -> Version -> TryGetDefinitionName -> IO (Maybe LSP.Uri)
unpackDoc miState packageDb lfVersion name = runMaybeT $ do
  let unitId = tgdnPackageUnitId name
  guard $ not $ unitIdIsBuiltin unitId
  packageDirs <- liftIO $ getDirectoryContents $ packageDb </> renderVersion lfVersion
  packageDir <- hoistMaybe $ find (isPrefixOf unitId) packageDirs
  let packageId = last $ splitWhen (=='-') packageDir
      unpackDir = unpackedDocsLocation miState </> unitId
  
  liftIO $ do
    createDirectoryIfMissing True unpackDir
    docsAreFresh <- doesFileExist $ unpackDir </> packageId

    unless docsAreFresh $ do
      -- Remove old docs and package-id markers
      removePathForcibly unpackDir
      createDirectory unpackDir
      -- Create new package-id marker
      writeFile (unpackDir </> packageId) ""
      generateDocFile (packageDb </> renderVersion lfVersion </> packageDir) packageDb lfVersion unpackDir
  pure $ LSP.Uri $ T.pack $ mconcat
    [ "daml:open-docs"
    , "?path=" <> (unpackDir </> "doc.html")
    , "&unitid=" <> unitId
    , "&anchor=" <> tryGetDefinitionNameToAnchor name
    ]

generateDocFile :: SdkVersioned => FilePath -> FilePath -> Version -> FilePath -> IO ()
generateDocFile importDir packageDb lfVersion unpackDir = do
  damlFiles <- getDamlRootFiles importDir
  let initialOpts = (defaultOptions $ Just lfVersion)
        { optImportPath = [importDir]
        , optPackageDbs = [packageDb]
        , optIfaceDir = Nothing
        , optIsGenerated = True
        , optDflagCheck = False
        }

  -- TODO Consider if this should be called via Shake
  -- Also consider if this should somehow defer to a given SDK version
  (_, pkgMap) <- generatePackageMap lfVersion Nothing [packageDb]
  exposedModules <- getExposedModules initialOpts (head damlFiles)

  -- some kind of formatting on the generated docs, they're very ugly <- consider using `md`, and the previewMarkdown command
  --   might end up simpler than webviews

  runDamlDoc DamldocOptions
    { do_inputFormat = InputDaml
    , do_compileOptions = initialOpts
        { optPackageImports =
            baseImports ++
            getDepImports pkgMap exposedModules
        -- (From Packaging.hs)
        -- When compiling dummy interface files for a data-dependency,
        -- we know all package flags so we don’t need to consult metadata.
        , optIgnorePackageMetadata = IgnorePackageMetadata True
        }
    , do_diagsLogger = diagnosticsLogger
    , do_outputPath = unpackDir </> "doc.html"
    , do_outputFormat = OutputDocs Html
    , do_docTemplate = Nothing
    , do_docIndexTemplate = Nothing
    , do_docHoogleTemplate = Nothing
    , do_transformOptions = defaultTransformOptions
    , do_inputFiles = damlFiles
    , do_docTitle = Nothing -- Consider renaming later
    , do_combine = True -- For now, we'll combine to make it easier
    , do_extractOptions = defaultExtractOptions
        { eo_anchorGenerators = AnchorGenerators
          { ag_moduleAnchor = \mod -> Anchor $ "m@" <> unModulename mod
          , ag_classAnchor = \mod tn -> Anchor $ "c@" <> unModulename mod <> "@" <> unTypename tn
          , ag_typeAnchor = \mod tn -> Anchor $ "t@" <> unModulename mod <> "@" <> unTypename tn
              -- Same type as class, as thats how get definition splits them
          , ag_constrAnchor = \mod tn -> Anchor $ "c@" <> unModulename mod <> "@" <> unTypename tn
          , ag_functionAnchor = \mod fn -> Anchor $ "f@" <> unModulename mod <> "@" <> unFieldname fn
          }
        }
    , do_baseURL = Nothing
    , do_hooglePath = Nothing
    , do_anchorPath = Nothing
    , do_externalAnchorBehaviour = ExternalAnchorFun $ \mPkgName (Anchor anchor) ->
        let args =
              BSLU.toString $ Aeson.encode
                [ Map.fromList @String 
                  [ ("anchor", T.unpack anchor)
                  , ("unitid", renderPackagename mPkgName)
                  , ("packagedb", packageDb)
                  , ("lfversion", renderVersion lfVersion)
                  ]
                ]
         in Just $ URI.URI "command:" Nothing "daml.getDocDefinition" ("?" <> args) ""
    , do_globalInternalExt = ""
    }

unitIdIsBuiltin :: String -> Bool
unitIdIsBuiltin unitId = any (`isPrefixOf` unitId) ["daml-prim", "daml-stdlib", "daml-script", "daml3-script"]

unpackedDocsLocation :: MultiIdeState -> FilePath
unpackedDocsLocation miState = misMultiPackageHome miState </> ".daml" </> "unpacked-docs"

renderPackagename :: Maybe Packagename -> String
renderPackagename = maybe "unknown-unit-id" (T.unpack . unPackagename)

tryGetDefinitionNameToAnchor :: TryGetDefinitionName -> String
tryGetDefinitionNameToAnchor TryGetDefinitionName {..} =
    nameSpace <> "@" <> tgdnModuleName <> "@" <> tgdnIdentifierName
  where
    nameSpace = case tgdnIdentifierNameSpace of
      VariableName -> "f"
      DataName -> "t"
      TypeVariableName -> "tv" -- Shouldn't ever happen, as type variables are always defined in the same module
      TypeCnstrOrClassName -> "c"

-- We assume the arguments are well formed
anchorToTryGetDefinitionName :: String -> String -> TryGetDefinitionName
anchorToTryGetDefinitionName anchor tgdnPackageUnitId = case splitWhen (=='@') anchor of
  [tagToNameSpace -> Just tgdnIdentifierNameSpace, tgdnModuleName, tgdnIdentifierName] -> TryGetDefinitionName {..}
  _ -> error "Invalid anchor format"
  where
    tagToNameSpace = \case
      "f" -> Just VariableName
      "t" -> Just DataName
      "tv" -> Just TypeVariableName
      "c" -> Just TypeCnstrOrClassName
      _ -> Nothing

data GetDocDefinition = GetDocDefinition
  { gddAnchor :: String
  , gddUnitId :: String
  , gddLfVersion :: String
  , gddPackageDb :: String
  }
