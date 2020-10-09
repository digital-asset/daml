-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE DerivingStrategies #-}
module TsCodeGenMain (main) where

import DA.Bazel.Runfiles (setRunfilesEnv)
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Daml.LF.Reader as DAR
import qualified DA.Pretty
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Map as Map
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified Data.Set.Lens as Set
import qualified Data.Text.Extended as T
import qualified Data.Text.IO as T
import qualified "zip-archive" Codec.Archive.Zip as Zip
import Data.Aeson hiding (Options)
import Data.Aeson.Encode.Pretty

import Control.Exception
import Control.Monad.Extra
import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Optics
import Data.Either
import Data.Tuple.Extra
import Data.List.Extra
import Data.Maybe
import Options.Applicative
import System.Directory
import System.Environment
import System.FilePath hiding ((<.>))

import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import qualified DA.Daml.Project.Types as DATypes

-- Version of the "@mojotech/json-type-validation" library we're using.
jtvVersion :: T.Text
jtvVersion = "^3.1.0"

data Options = Options
    { optInputDars :: [FilePath]
    , optOutputDir :: FilePath
    , optScope :: Scope -- Defaults to 'daml.js'.
    }

optionsParser :: Parser Options
optionsParser = Options
    <$> some ( argument str
        (  metavar "DAR-FILES"
        <> help "DAR files to generate TypeScript bindings for"
        ) )
    <*> strOption
        (  short 'o'
        <> metavar "DIR"
        <> help "Output directory for the generated packages"
        )
    <*> (Scope . ("@" <>) <$> strOption
        (  short 's'
        <> metavar "SCOPE"
        <> value "daml.js"
        <> help "The NPM scope name for the generated packages; defaults to daml.js"
        ))

optionsParserInfo :: ParserInfo Options
optionsParserInfo = info (optionsParser <**> helper)
    (  fullDesc
    <> progDesc "Generate TypeScript bindings from a DAR"
    )

-- Build a list of packages from a list of DAR file paths.
readPackages :: [FilePath] -> IO [(PackageId, (Package, Maybe PackageName))]
readPackages dars = concatMapM darToPackages dars
  where
    darToPackages :: FilePath -> IO [(PackageId, (Package, Maybe PackageName))]
    darToPackages dar = do
      dar <- B.readFile dar
      let archive = Zip.toArchive $ BSL.fromStrict dar
      dalfs <- either fail pure $ DAR.readDalfs archive
      DAR.DalfManifest{packageName} <- either fail pure $ DAR.readDalfManifest archive
      packageName <- pure (PackageName . T.pack <$> packageName)
      forM ((DAR.mainDalf dalfs, packageName) : map (, Nothing) (DAR.dalfs dalfs)) $
        \(dalf, mbPkgName) -> do
          (pkgId, pkg) <- either (fail . show)  pure $ Archive.decodeArchive Archive.DecodeAsMain (BSL.toStrict dalf)
          pure (pkgId, (pkg, mbPkgName))

-- Work out the set of packages we have to generate script for and by
-- what names.
mergePackageMap :: [(PackageId, (Package, Maybe PackageName))] ->
                   Either T.Text (Map.Map PackageId (Maybe PackageName, Package))
mergePackageMap ps = foldM merge Map.empty ps
  where
    merge :: Map.Map PackageId (Maybe PackageName, Package) ->
                  (PackageId, (Package, Maybe PackageName)) ->
                  Either T.Text (Map.Map PackageId (Maybe PackageName, Package))
    merge pkgs (pkgId, (pkg, mbPkgName)) = do
        let pkgNames = mapMaybe fst (Map.elems pkgs)
        -- Check if there is a package with the same name but a
        -- different package id.
        whenJust mbPkgName $ \name -> when (pkgId `Map.notMember` pkgs && name `elem` pkgNames) $
            Left $ "Duplicate name '" <> unPackageName name <> "' for different packages detected"
        let update mbOld = case mbOld of
                Nothing -> pure (Just (mbPkgName, pkg))
                Just (mbOldPkgName, _) -> do
                    -- Check if we have colliding names for the same
                    -- package.
                    whenJust (liftA2 (,) mbOldPkgName mbPkgName) $ \(name1, name2) ->
                        when (name1 /= name2) $ Left $ "Different names ('" <> unPackageName name1 <> "' and '" <> unPackageName name2 <> "') for the same package detected"
                    pure (Just (mbOldPkgName <|> mbPkgName, pkg))
        Map.alterF update pkgId pkgs

-- Write packages for all the DALFs in all the DARs.
main :: IO ()
main = do
    -- Save the runfiles environment to work around
    -- https://gitlab.haskell.org/ghc/ghc/-/issues/18418.
    setRunfilesEnv
    withProgName "daml codegen js" $ do
        opts@Options{..} <- customExecParser (prefs showHelpOnError) optionsParserInfo
        sdkVersionOrErr <- DATypes.parseVersion . T.pack . fromMaybe "0.0.0" <$> getSdkVersionMaybe
        sdkVersion <- case sdkVersionOrErr of
              Left _ -> fail "Invalid SDK version"
              Right v -> pure v
        pkgs <- readPackages optInputDars
        case mergePackageMap pkgs of
          Left err -> fail . T.unpack $ err
          Right pkgMap -> do
              forM_ (Map.toList pkgMap) $
                \(pkgId, (mbPkgName, pkg)) -> do
                     let id = unPackageId pkgId
                         pkgName = packageNameText pkgId mbPkgName
                     let pkgDesc = case mbPkgName of
                           Nothing -> id
                           Just pkgName -> unPackageName pkgName <> " (hash: " <> id <> ")"
                     T.putStrLn $ "Generating " <> pkgDesc
                     daml2js Daml2jsParams{..}

packageNameText :: PackageId -> Maybe PackageName -> T.Text
packageNameText pkgId mbPkgIdent = maybe (unPackageId pkgId) unPackageName mbPkgIdent

newtype Scope = Scope {unScope :: T.Text}
newtype Dependency = Dependency {_unDependency :: T.Text} deriving (Eq, Ord)

data Daml2jsParams = Daml2jsParams
  { opts :: Options  -- cli args
  , pkgMap :: Map.Map PackageId (Maybe PackageName, Package)
  , pkgId :: PackageId
  , pkg :: Package
  , pkgName :: T.Text
  , sdkVersion :: SdkVersion
  }

-- Write the files for a single package.
daml2js :: Daml2jsParams -> IO ()
daml2js Daml2jsParams {..} = do
    let Options {..} = opts
        scopeDir = optOutputDir
          -- The directory into which we generate packages e.g. '/path/to/daml2js'.
        packageDir = scopeDir </> T.unpack pkgName
          -- The directory into which we write this package e.g. '/path/to/daml2js/davl-0.0.4'.
        packageSrcDir = packageDir </> "lib"
          -- Where the source files of this package are written e.g. '/path/to/daml2js/davl-0.0.4/lib'.
        scope = optScope
          -- The scope e.g. '@daml.js'.
          -- We use this, for example, when generating import declarations e.g.
          --   import * as pkgd14e08_DA_Internal_Template from '@daml.js/d14e08/lib/DA/Internal/Template';
    createDirectoryIfMissing True packageSrcDir
    -- Write .ts files for the package and harvest references to
    -- foreign packages as we do.
    (nonEmptyModNames, dependenciesSets) <- unzip <$> mapM (writeModuleTs packageSrcDir scope) (NM.toList (packageModules pkg))
    writeIndexTs pkgId packageSrcDir (catMaybes nonEmptyModNames)
    let dependencies = Set.toList (Set.unions dependenciesSets)
    -- Now write package metadata.
    writeTsConfig packageDir
    writePackageJson packageDir sdkVersion scope dependencies
    where
      -- Write the .ts file for a single DAML-LF module.
      writeModuleTs :: FilePath -> Scope -> Module -> IO (Maybe ModuleName, Set.Set Dependency)
      writeModuleTs packageSrcDir scope mod = do
        case genModule pkgMap scope pkgId mod of
          Nothing -> pure (Nothing, Set.empty)
          Just ((jsSource, tsDecls), ds) -> do
            let outputDir = packageSrcDir </> joinPath (map T.unpack (unModuleName (moduleName mod)))
            createDirectoryIfMissing True outputDir
            T.writeFileUtf8 (outputDir </> "module.js") jsSource
            T.writeFileUtf8 (outputDir </> "module.d.ts") tsDecls
            pure (Just (moduleName mod), ds)

-- Generate the .ts content for a single module.
genModule :: Map.Map PackageId (Maybe PackageName, Package) ->
     Scope -> PackageId -> Module -> Maybe ((T.Text, T.Text), Set.Set Dependency)
genModule pkgMap (Scope scope) curPkgId mod
  | null serDefs =
    Nothing -- If no serializable types, nothing to do.
  | otherwise =
    let (decls, refs) = unzip (map (genDataDef curPkgId mod tpls) serDefs)
        imports = (PRSelf, modName) `Set.delete` Set.unions refs
        (internalImports, externalImports) = splitImports imports
        rootPath = map (const "..") (unModuleName modName)
        makeMod jsSyntax body = T.unlines $ intercalate [""] $ filter (not . null) $
          modHeader jsSyntax
          : map (externalImportDecl jsSyntax pkgMap) (Set.toList externalImports)
          : map (internalImportDecl jsSyntax rootPath) internalImports
          : body

        (jsBody, tsDeclsBody) = unzip $ map (unzip . map renderTsDecl) decls
        depends = Set.map (Dependency . pkgRefStr pkgMap) externalImports
   in Just ((makeMod ES5 jsBody, makeMod ES6 tsDeclsBody), depends)
  where
    modName = moduleName mod
    tpls = moduleTemplates mod
    serDefs = defDataTypes mod
    modHeader ES5 = commonjsPrefix ++
      [ "/* eslint-disable-next-line no-unused-vars */"
      , "var jtv = require('@mojotech/json-type-validation');"
      , "/* eslint-disable-next-line no-unused-vars */"
      , "var damlTypes = require('@daml/types');"
      , "/* eslint-disable-next-line no-unused-vars */"
      , "var damlLedger = require('@daml/ledger');"
      ]
    modHeader ES6 =
      [ "// Generated from " <> modPath (unModuleName modName) <> ".daml"
      , "/* eslint-disable @typescript-eslint/camelcase */"
      , "/* eslint-disable @typescript-eslint/no-namespace */"
      , "/* eslint-disable @typescript-eslint/no-use-before-define */"
      , "import * as jtv from '@mojotech/json-type-validation';"
      , "import * as damlTypes from '@daml/types';"
      , "/* eslint-disable-next-line @typescript-eslint/no-unused-vars */"
      , "import * as damlLedger from '@daml/ledger';"
      ]

    -- Split the imports into those from the same package and those
    -- from another package.
    splitImports :: Set.Set ModuleRef -> ([ModuleName], Set.Set PackageId)
    splitImports imports =
      let classifyImport (pkgRef, modName) = case pkgRef of
            PRSelf -> Left modName
            PRImport pkgId -> Right pkgId
      in
      second Set.fromList (partitionEithers (map classifyImport (Set.toList imports)))

    -- Calculate an import declaration for a module from the same package.
    internalImportDecl :: JSSyntax -> [T.Text] -> ModuleName -> T.Text
    internalImportDecl jsSyntax rootPath modName =
        importStmt
            jsSyntax
            (genModuleRef (PRSelf, modName))
            (modPath (rootPath ++ unModuleName modName ++ ["module"]))

    -- Calculate an import declaration for a module from another package.
    externalImportDecl
        :: JSSyntax
        -> Map.Map PackageId (Maybe PackageName, Package)
        -> PackageId
        -> T.Text
    externalImportDecl jsSyntax pkgMap pkgId =
        importStmt jsSyntax (pkgVar pkgId) (scope <> "/" <> pkgRefStr pkgMap pkgId)

    -- Produce a package name for a package ref.
    pkgRefStr :: Map.Map PackageId (Maybe PackageName, Package) -> PackageId -> T.Text
    pkgRefStr pkgMap pkgId =
        case Map.lookup pkgId pkgMap of
          Nothing -> error "IMPOSSIBLE : package map malformed"
          Just (mbPkgName, _) -> packageNameText pkgId mbPkgName

importStmt :: JSSyntax -> T.Text -> T.Text -> T.Text
importStmt ES6 asName impName =
    "import * as " <>  asName <> " from '" <> impName <> "';"
importStmt ES5 asName impName =
    "var " <> asName <> " = require('" <> impName <> "');"

defDataTypes :: Module -> [DefDataType]
defDataTypes mod = filter (getIsSerializable . dataSerializable) (NM.toList (moduleDataTypes mod))

genDataDef :: PackageId -> Module -> NM.NameMap Template -> DefDataType -> ([TsDecl], Set.Set ModuleRef)
genDataDef curPkgId mod tpls def = case unTypeConName (dataTypeCon def) of
    [] -> error "IMPOSSIBLE: empty type constructor name"
    _: _: _: _ -> error "IMPOSSIBLE: multi-part type constructor of more than two names"

    [conName] -> genDefDataType curPkgId conName mod tpls def
    [c1, c2] -> ([DeclNamespace c1 tyDecls], refs)
      where
        (decls, refs) = genDefDataType curPkgId c2 mod tpls def
        tyDecls = [d | DeclTypeDef d <- decls]

-- | The typescript declarations we produce.
data TsDecl
    = DeclTemplateDef TemplateDef
    | DeclSerializableDef SerializableDef
    | DeclTypeDef TypeDef
    | DeclTemplateNamespace TemplateNamespace
    | DeclTemplateRegistration TemplateRegistration
    | DeclNamespace T.Text [TypeDef]
    -- ^ Note that we special-case some namespaces, e.g., the template namespace
    -- that always have fixed contents. This constructor is only used for the namespace
    -- for sums of products.

renderTsDecl :: TsDecl -> (T.Text, T.Text)
renderTsDecl = \case
    DeclTemplateDef t -> renderTemplateDef t
    DeclSerializableDef t -> renderSerializableDef t
    DeclTypeDef t -> ("", "export declare " <> renderTypeDef t)
    DeclTemplateNamespace t -> ("", renderTemplateNamespace t)
    DeclTemplateRegistration t -> (renderTemplateRegistration t, "")
    DeclNamespace t decls -> ("", T.unlines $ concat
        [ [ "export namespace " <> t <> " {" ]
        , [ "  " <> l | d <- decls, l <- T.lines (renderTypeDef d) ]
        , [ "} //namespace " <> t ]
        ])


-- | Namespace containing type synonyms for Key, CreatedEvent, ArchivedEvent and Event
-- for the given template.
data TemplateNamespace = TemplateNamespace
  { tnsName :: T.Text
  , tnsMbKeyDef :: Maybe TypeRef
  }

renderTemplateNamespace :: TemplateNamespace -> T.Text
renderTemplateNamespace TemplateNamespace{..} = T.unlines $ concat
    [ [ "export declare namespace " <> tnsName <> " {" ]
    , [ "  export type Key = " <> fst (genType keyDef) | Just keyDef <- [tnsMbKeyDef] ]
    , [ "  export type CreateEvent = damlLedger.CreateEvent" <> tParams [tnsName, tK, tI]
      , "  export type ArchiveEvent = damlLedger.ArchiveEvent" <> tParams [tnsName, tI]
      , "  export type Event = damlLedger.Event" <> tParams [tnsName, tK, tI]
      , "}"
      ]
    ]
  where
    tK = maybe "undefined" (const (tnsName <.> "Key")) tnsMbKeyDef
    tI = "typeof " <> tnsName <.> "templateId"
    tParams xs = "<" <> T.intercalate ", " xs <> ">"

data TemplateRegistration = TemplateRegistration T.Text

renderTemplateRegistration :: TemplateRegistration -> T.Text
renderTemplateRegistration (TemplateRegistration t) = T.unlines
  [ "damlTypes.registerTemplate(exports." <> t <> ");" ]

data TemplateDef = TemplateDef
  { tplName :: T.Text
  , tplPkgId :: PackageId
  , tplModule :: ModuleName
  , tplDecoder :: Decoder
  , tplKeyDecoder :: Maybe Decoder
  -- ^ Nothing if we do not have a key.
  , tplChoices' :: [ChoiceDef]
  }

renderTemplateDef :: TemplateDef -> (T.Text, T.Text)
renderTemplateDef TemplateDef{..} =
    let jsSource = T.unlines $ concat
          [ [ "exports." <> tplName <> " = {"
            , "  templateId: '" <> templateId <> "',"
            , "  keyDecoder: " <> renderDecoder (DecoderLazy keyDec) <> ","
            , "  decoder: " <> renderDecoder (DecoderLazy tplDecoder) <> ","
            ]
          , concat
            [ [ "  " <> chcName' <> ": {"
              , "    template: function () { return exports." <> tplName <> "; },"
              , "    choiceName: '" <> chcName' <> "',"
              , "    argumentDecoder: " <> renderDecoder (DecoderLazy (DecoderRef chcArgTy)) <> ","
              , "    resultDecoder: " <> renderDecoder (DecoderLazy (DecoderRef chcRetTy)) <> ","
              , "  },"
              ]
            | ChoiceDef{..} <- tplChoices'
            ]
          , [ "};" ]
          ]
        tsDecl = T.unlines $ concat
          [ [ "export declare const " <> tplName <> ":"
            , "  damlTypes.Template<" <> tplName <> ", " <> keyTy <> ", '" <> templateId <> "'> & {"
            ]
          , [ "  " <> chcName' <> ": damlTypes.Choice<" <>
              tplName <> ", " <>
              fst (genType chcArgTy) <> ", " <>
              fst (genType chcRetTy) <> ", " <>
              keyTy <> ">;" | ChoiceDef{..} <- tplChoices' ]
          , [ "};" ]
          ]
    in (jsSource, tsDecl)
  where (keyTy, keyDec) = case tplKeyDecoder of
            Nothing -> ("undefined", DecoderConstant ConstantUndefined)
            Just d -> (tplName <> ".Key", DecoderLazy d)
        templateId =
            unPackageId tplPkgId <> ":" <>
            T.intercalate "." (unModuleName tplModule) <> ":" <>
            tplName

data ChoiceDef = ChoiceDef
  { chcName' :: T.Text
  , chcArgTy :: TypeRef
  , chcRetTy :: TypeRef
  }

data SerializableDef = SerializableDef
  { serName :: T.Text
  , serParams :: [T.Text]
  -- ^ Type parameters.
  , serKeys :: [T.Text]
  -- ^ Keys for enums. Note that enums never have type parameters
  -- but for simplicity we do not express this in this type.
  , serDecoder :: Decoder
  , serNestedDecoders :: [(T.Text, Decoder)]
  -- ^ For sums of products, e.g., `data X = Y { a : Int }
  }

renderSerializableDef :: SerializableDef -> (T.Text, T.Text)
renderSerializableDef SerializableDef{..}
  | null serParams =
    let tsDecl = T.unlines $ concat
            [ [ "export declare const " <> serName <> ":"
              , "  damlTypes.Serializable<" <> serName <> "> & {"
              ]
            , [ "  " <> n <> ": damlTypes.Serializable<" <> serName <.> n <> ">;" | (n, _) <- serNestedDecoders ]
            , [ "  }"
              ]
            , [ "& { readonly keys: " <> serName <> "[] } & { readonly [e in " <> serName <> "]: e }" | notNull serKeys ]
            , [ ";"]
            ]
        jsDecl = T.unlines $ concat
          [ ["exports." <> serName <> " = {"]
          , [ "  " <> k <> ": " <> "'" <> k <> "'," | k <- serKeys ]
          , [ "  keys: [" <> T.concat (map (\s -> "'" <> s <> "',") serKeys) <> "]," | notNull serKeys ]
          , [ "  decoder: " <> renderDecoder (DecoderLazy serDecoder) <> ","
            ]
          , concat $
            [ [ "  " <> n <> ":({"
              , "    decoder: " <> renderDecoder (DecoderLazy d) <> ","
              , "  }),"
              ]
            | (n, d) <- serNestedDecoders
            ]
          , [ "};" ]
          ]
    in (jsDecl, tsDecl)
  | otherwise = assert (null serKeys) $
    let tsDecl = T.unlines $
            -- If we have type parameters, the serializable definition is
            -- a function and we generate extra properties on that function
            -- for each nested decoder.
            [ "export declare const " <> serName <> " :"
            , "  (" <> tyArgs <> " => damlTypes.Serializable<" <> serName <> tyParams <> ">) & {"
            ] ++
            [ "  " <> n <> ": (" <> tyArgs <> " => damlTypes.Serializable<" <> serName <.> n <> tyParams <> ">);"
            | (n, _) <- serNestedDecoders
            ] ++
            [ "};"
            ]
        jsSource = T.unlines $
            -- If we have type parameters, the serializable definition is
            -- a function and we generate extra properties on that function
            -- for each nested decoder.
            [ "exports" <.> serName <> " = function " <> jsTyArgs <> " { return ({"
            , "  decoder: " <> renderDecoder (DecoderLazy serDecoder) <> ","
            , "}); };"
            ] <> concat
            [ [ "exports" <.> serName <.> n <> " = function " <> jsTyArgs <> " { return ({"
              , "  decoder: " <> renderDecoder (DecoderLazy d) <> ","
              , "}); };"
              ]
            | (n, d) <- serNestedDecoders
            ]
    in (jsSource, tsDecl)
  where tyParams = "<" <> T.intercalate ", " serParams <> ">"
        tyArgs = tyParams <> "(" <> T.intercalate ", " (map (\name -> name <> ": damlTypes.Serializable<" <> name <> ">") serParams) <> ")"
        jsTyArgs = "(" <> T.intercalate ", " serParams <> ")"

data TypeRef = TypeRef
  { _refFromModule :: ModuleName
  , refType :: Type
  }

data Decoder
    = DecoderOneOf T.Text [Decoder]
    | DecoderObject [(T.Text, Decoder)]
    | DecoderConstant DecoderConstant
    | DecoderRef TypeRef -- ^ Reference to an object with a .decoder field
    | DecoderLazy Decoder -- ^ Lazy decoder, we need this to avoid infinite loops
    -- on recursive types. We insert this in every variant, Optional, List and TextMap
    -- which are the only ways to construct terminating recursive types.

data DecoderConstant
    = ConstantUndefined
    | ConstantString T.Text -- ^ String literal
    | ConstantRef T.Text -- ^ Variable reference

renderDecoderConstant :: DecoderConstant -> T.Text
renderDecoderConstant = \case
    ConstantUndefined -> "undefined"
    ConstantString s -> "'" <> s <> "'"
    ConstantRef v -> v

renderDecoder :: Decoder -> T.Text
renderDecoder = \case
    DecoderOneOf _constr branches ->
        "jtv.oneOf(" <>
        T.intercalate ", " (map renderDecoder branches) <>
        ")"
    DecoderObject fields ->
        "jtv.object({" <>
        T.concat (map (\(name, d) -> name <> ": " <> renderDecoder d <> ", ") fields) <>
        "})"
    DecoderConstant c -> "jtv.constant(" <> renderDecoderConstant c <> ")"
    DecoderRef t -> snd (genType t) <> ".decoder"
    DecoderLazy d -> "damlTypes.lazyMemo(function () { return " <> renderDecoder d <> "; })"

data TypeDef
    = UnionDef T.Text [T.Text] [(T.Text, TypeRef)]
    | ObjectDef T.Text [T.Text] [(T.Text, TypeRef)]
    | EnumDef T.Text [T.Text] [T.Text]

renderTypeDef :: TypeDef -> T.Text
renderTypeDef = \case
    UnionDef t args bs -> T.unlines $ concat
        [ [ "type " <> ty t args <> " =" ]
        , [ "  |  { tag: '" <> k <> "'; value: " <> fst (genType t) <> " }" | (k, t) <- bs ]
        , [ ";" ]
        ]
    ObjectDef t args fs -> T.unlines $ concat
        [ [ "type " <> ty t args <> " = {" ]
        , [ "  " <> k <> ": " <> fst (genType t) <> ";" | (k, t) <- fs ]
        , [ "};" ]
        ]
    EnumDef t args fs -> T.unlines $ concat
        [ [ "type " <> ty t args <> " =" ]
        , [ "  | '" <> f <> "'" | f <- fs ]
        , [ ";" ]
        ]
  where ty t args
            | null args = t
            | otherwise = t <> "<" <> T.intercalate ", " args <> ">"

-- | Generate the Serializable definition for a datatype.
-- Note that for templates we do not use this directly since the Template definition
-- subsumes this.
genSerializableDef :: PackageId -> T.Text -> Module -> DefDataType -> SerializableDef
genSerializableDef curPkgId conName mod def =
    case dataCons def of
        DataVariant bs ->
            let typ = conName <> typeParams
            in SerializableDef
                 { serName = conName
                 , serParams = paramNames
                 , serKeys = []
                 , serDecoder = DecoderOneOf typ (map genBranch bs)
                 , serNestedDecoders =
                   [ (name, serDecoder (genSerializableDef curPkgId (conName <.> name) mod b)) | (name, b) <- nestedDefDataTypes ]
                 }
        DataEnum enumCons ->
            let cs = map unVariantConName enumCons
            in SerializableDef
                 { serName = conName
                 , serParams = []
                 , serKeys = cs
                 , serDecoder = DecoderOneOf conName [DecoderConstant (ConstantRef ("exports" <.> conName <.> cons)) | cons <- cs]
                 , serNestedDecoders = []
                 }
        DataRecord fields ->
            let (fieldNames, fieldTypesLf) = unzip [(unFieldName x, t) | (x, t) <- fields]
                fieldSers = map (\t -> TypeRef (moduleName mod) t) fieldTypesLf
            in SerializableDef
                 { serName = conName
                 , serParams = paramNames
                 , serKeys = []
                 , serDecoder = DecoderObject [(x, DecoderRef ser) | (x, ser) <- zip fieldNames fieldSers]
                 , serNestedDecoders = []
                 }
  where
    paramNames = map (unTypeVarName . fst) (dataParams def)
    typeParams
        | null paramNames = ""
        | otherwise = "<" <> T.intercalate ", " paramNames <> ">"
    genBranch (VariantConName cons, t) =
        DecoderObject
            [ ("tag", DecoderConstant (ConstantString cons))
            , ("value", DecoderRef $ TypeRef (moduleName mod) t)
            ]
    nestedDefDataTypes =
        [ (sub, def)
        | def <- defDataTypes mod
        , [sup, sub] <- [unTypeConName (dataTypeCon def)], sup == conName
        ]

genTypeDef :: T.Text -> Module -> DefDataType -> TypeDef
genTypeDef conName mod def =
    case dataCons def of
        DataVariant bs ->
            UnionDef
                conName
                paramNames
                [ (cons, typ)
                | (VariantConName cons, t) <- bs, let typ = TypeRef (moduleName mod) t
                ]
        DataEnum enumCons ->
            EnumDef
                conName
                paramNames
                (map unVariantConName enumCons)
        DataRecord fields ->
            ObjectDef
                conName
                paramNames
                [ (n, TypeRef (moduleName mod) ty) | (FieldName n, ty) <- fields ]

  where
    paramNames = map (unTypeVarName . fst) (dataParams def)

genDefDataType :: PackageId -> T.Text -> Module -> NM.NameMap Template -> DefDataType -> ([TsDecl], Set.Set ModuleRef)
genDefDataType curPkgId conName mod tpls def =
    case dataCons def of
        DataVariant bs ->
          let
            typeDesc = genTypeDef conName mod def
            serDesc = genSerializableDef curPkgId conName mod def
          in ([DeclTypeDef typeDesc, DeclSerializableDef serDesc], Set.unions $ map (Set.setOf typeModuleRef . snd) bs)
        DataEnum _ ->
          let typeDesc = genTypeDef conName mod def
              serDesc = genSerializableDef curPkgId conName mod def
          in
          ([DeclTypeDef typeDesc, DeclSerializableDef serDesc], Set.empty)
        DataRecord fields ->
            let (fieldNames, fieldTypesLf) = unzip [(unFieldName x, t) | (x, t) <- fields]
                fieldSers = map (TypeRef (moduleName mod)) fieldTypesLf
                fieldRefs = map (Set.setOf typeModuleRef . snd) fields
                typeDesc = genTypeDef conName mod def
            in
            case NM.lookup (dataTypeCon def) tpls of
                Nothing -> ([DeclTypeDef typeDesc, DeclSerializableDef $ genSerializableDef curPkgId conName mod def], Set.unions fieldRefs)
                Just tpl ->
                    let (chcs, chcRefs) = unzip
                            [(ChoiceDef (unChoiceName (chcName chc)) argTy rTy, Set.union argRefs retRefs)
                            | chc <- NM.toList (tplChoices tpl)
                            , let argTy = TypeRef (moduleName mod) (snd (chcArgBinder chc))
                            , let rTy = TypeRef (moduleName mod) (chcReturnType chc)
                            , let argRefs = Set.setOf typeModuleRef (refType argTy)
                            , let retRefs = Set.setOf typeModuleRef (refType rTy)
                            ]
                        (keyDecoder, keyRefs) = case tplKey tpl of
                            Nothing -> (Nothing, Set.empty)
                            Just key ->
                                let keyType = tplKeyType key
                                in
                                (Just (DecoderRef $ TypeRef (moduleName mod) keyType), Set.setOf typeModuleRef keyType)
                        dict = TemplateDef
                            { tplName = conName
                            , tplPkgId = curPkgId
                            , tplModule = moduleName mod
                            , tplDecoder = DecoderObject [(x, DecoderRef ser) | (x, ser) <- zip fieldNames fieldSers]
                            , tplKeyDecoder = keyDecoder
                            , tplChoices' = chcs
                            }
                        associatedTypes = TemplateNamespace
                          { tnsName = conName
                          , tnsMbKeyDef = TypeRef (moduleName mod) . tplKeyType <$> tplKey tpl
                          }
                        registrations = TemplateRegistration conName
                        refs = Set.unions (fieldRefs ++ keyRefs : chcRefs)
                    in
                    ([DeclTypeDef typeDesc, DeclTemplateDef dict, DeclTemplateNamespace associatedTypes, DeclTemplateRegistration registrations], refs)

infixr 6 <.> -- This is the same fixity as '<>'.
(<.>) :: T.Text -> T.Text -> T.Text
(<.>) u v = u <> "." <> v

-- | Returns a pair of the type and a reference to the
-- serializer object.
genType :: TypeRef -> (T.Text, T.Text)
genType (TypeRef curModName t) = go t
  where
    go = \case
        TVar v -> dupe (unTypeVarName v)
        TUnit -> ("{}", "damlTypes.Unit")
        TBool -> ("boolean", "damlTypes.Bool")
        TInt64 -> dupe "damlTypes.Int"
        TDecimal -> dupe "damlTypes.Decimal"
        TNumeric (TNat n) -> (
            "damlTypes.Numeric"
          , "damlTypes.Numeric(" <> T.pack (show (fromTypeLevelNat n :: Integer)) <> ")"
          )
        TText -> ("string", "damlTypes.Text")
        TTimestamp -> dupe "damlTypes.Time"
        TParty -> dupe "damlTypes.Party"
        TDate -> dupe "damlTypes.Date"
        TList t ->
            let (t', ser) = go t
            in
            (t' <> "[]", "damlTypes.List(" <> ser <> ")")
        TOptional t ->
            let (t', ser) = go t
            in
            ("damlTypes.Optional<" <> t' <> ">", "damlTypes.Optional(" <> ser <> ")")
        TTextMap t  ->
            let (t', ser) = go t
            in
            ("{ [key: string]: " <> t' <> " }", "damlTypes.TextMap(" <> ser <> ")")
        TUpdate _ -> error "IMPOSSIBLE: Update not serializable"
        TScenario _ -> error "IMPOSSIBLE: Scenario not serializable"
        TContractId t ->
            let (t', ser) = go t
            in
            ("damlTypes.ContractId<" <> t' <> ">", "damlTypes.ContractId(" <> ser <> ")")
        TConApp con ts ->
            let (con', ser) = genTypeCon curModName con
                (ts', sers) = unzip (map go ts)
            in
            if null ts
                then (con', ser)
                else
                    ( con' <> "<" <> T.intercalate ", " ts' <> ">"
                    , ser <> "(" <> T.intercalate ", " sers <> ")"
                    )
        TCon _ -> error "IMPOSSIBLE: lonely type constructor"
        TSynApp{} -> error "IMPOSSIBLE: type synonym not serializable"
        t@TApp{} -> error $ "IMPOSSIBLE: type application not serializable - " <> DA.Pretty.renderPretty t
        TBuiltin t -> error $ "IMPOSSIBLE: partially applied primitive type not serializable - " <> DA.Pretty.renderPretty t
        TForall{} -> error "IMPOSSIBLE: universally quantified type not serializable"
        TStruct{} -> error "IMPOSSIBLE: structural record not serializable"
        TNat{} -> error "IMPOSSIBLE: standalone type level natural not serializable"

-- | Pair of a reference to the type and a reference to the serializer.
-- Note that the serializer is in JS file whereas the type is in the TS
-- declaration file. Therefore they refer to things in the current module
-- differently.
genTypeCon :: ModuleName -> Qualified TypeConName -> (T.Text, T.Text)
genTypeCon curModName (Qualified pkgRef modName conParts) =
    case unTypeConName conParts of
        [] -> error "IMPOSSIBLE: empty type constructor name"
        _: _: _: _ -> error "TODO(MH): multi-part type constructor names"
        [c1 ,c2]
          | modRef == (PRSelf, curModName) ->
            (c1 <.> c2, "exports" <.> c1 <.> c2)
          | otherwise -> dupe $ genModuleRef modRef <> c1 <.> c2
        [conName]
          | modRef == (PRSelf, curModName) ->
            (conName, "exports" <.> conName)
          | otherwise -> dupe $ genModuleRef modRef <.> conName
     where
       modRef = (pkgRef, modName)

pkgVar :: PackageId -> T.Text
pkgVar pkgId = "pkg" <> unPackageId pkgId

genModuleRef :: ModuleRef -> T.Text
genModuleRef (pkgRef, modName) = case pkgRef of
    PRSelf -> T.intercalate "_" name
    PRImport pkgId -> T.intercalate "." (pkgVar pkgId : name)
  where
    name = unModuleName modName

-- Calculate a filepath from a module name e.g. 'modPath [".", "A",
-- "B"]' is "./A/B".
modPath :: [T.Text] -> T.Text
modPath parts = T.intercalate "/" parts

writeTsConfig :: FilePath -> IO ()
writeTsConfig dir =
  BSL.writeFile (dir </> "tsconfig.json") $ encodePretty tsConfig
  where
    tsConfig :: Value
    tsConfig = object
      [ "compilerOptions" .= object
        [ "target" .= ("es5" :: T.Text)
        , "lib" .= (["dom", "es2015"] :: [T.Text])
        , "skipLibCheck" .= True
        , "strict" .= True
        , "noUnusedLocals" .= False
        , "noImplicitReturns" .= True
        , "noFallthroughCasesInSwitch" .= True
        , "outDir" .= ("lib" :: T.Text)
        , "module" .= ("commonjs" :: T.Text)
        , "declaration" .= True
        , "sourceMap" .= True
        ]
      , "include" .= (["src/**/*.ts"] :: [T.Text])
      ]

packageJsonDependencies :: SdkVersion -> Scope -> [Dependency] -> Value
packageJsonDependencies sdkVersion (Scope scope) dependencies = object $
    [ "@mojotech/json-type-validation" .= jtvVersion
    , "@daml/types" .= versionToText sdkVersion
    , "@daml/ledger" .= versionToText sdkVersion
    ] ++
    [ (scope <> "/" <> pkgName) .= ("file:../" <> pkgName) | Dependency pkgName <- dependencies ]

writePackageJson :: FilePath -> SdkVersion -> Scope -> [Dependency] -> IO ()
writePackageJson packageDir sdkVersion scope dependencies =
  let packageJson = object
        [ "private" .= True
        , "name" .= (unScope scope <> "/" <> T.pack (takeFileName packageDir))
        , "version" .= versionToText sdkVersion
        , "license" .= ("UNLICENSED" :: T.Text)
        , "main" .= ("lib/index.js" :: T.Text)
        , "types" .= ("lib/index.d.ts" :: T.Text)
        , "description" .= ("Generated by `daml codegen js` from SDK " <> versionToText sdkVersion)
        , "dependencies" .= packageJsonDependencies sdkVersion scope dependencies
        ]
  in
  BSL.writeFile (packageDir </> "package.json") (encodePretty packageJson)

writeIndexTs :: PackageId -> FilePath -> [ModuleName] -> IO ()
writeIndexTs pkgId packageSrcDir modNames =
  processIndexTree pkgId packageSrcDir (buildIndexTree modNames)

-- NOTE(MH): The module structure of a DAML package can have "holes", i.e.,
-- you can have modules `A` and `A.B.C` but no module `A.B`. We call such a
-- module `A.B` a "virtual module". In order to use ES2015 modules and form
-- a hierarchy of these, we need to produce JavaScript modules for virtual
-- DAML modules as well. To this end, we assemble the names of all modules
-- into a tree structure where each node is marked whether is is virtual or
-- not. Afterwards, we take this tree structure and write a resembling
-- directory structure full of `index.ts` files to disk.
data IndexTree = IndexTree
  { isVirtual :: Bool
  , children :: Map.Map T.Text IndexTree
  }

buildIndexTree :: [ModuleName] -> IndexTree
buildIndexTree = foldl' merge empty . map path
  where
    empty = IndexTree{isVirtual = True, children = Map.empty}
    leaf = IndexTree{isVirtual = False, children = Map.empty}

    path :: ModuleName -> IndexTree
    path = foldr (\name node -> empty{children = Map.singleton name node}) leaf . unModuleName

    merge :: IndexTree -> IndexTree -> IndexTree
    merge t1 t2 = IndexTree
      { isVirtual = isVirtual t1 && isVirtual t2
      , children = Map.unionWith merge (children t1) (children t2)
      }

processIndexTree :: PackageId -> FilePath -> IndexTree -> IO ()
processIndexTree pkgId srcDir root = do
  T.writeFileUtf8 (srcDir </> "index.d.ts") $ T.unlines $
    reexportChildren ES6 root ++
    [ "export declare const packageId = '" <> unPackageId pkgId <> "';" ]
  T.writeFileUtf8 (srcDir </> "index.js") $ T.unlines $
    commonjsPrefix ++
    reexportChildren ES5 root ++
    [ "exports.packageId = '" <> unPackageId pkgId <> "';" ]
  processChildren (ModuleName []) root
  where
    processChildren :: ModuleName -> IndexTree -> IO ()
    processChildren parentModName parent =
      forM_ (Map.toList (children parent)) $ \(name, node) -> do
        let modName = ModuleName (unModuleName parentModName ++ [name])
        let modDir = srcDir </> joinPath (map T.unpack (unModuleName modName))
        createDirectoryIfMissing True modDir
        let indexContent jsSyntax = T.unlines $
                (case jsSyntax of
                     ES6 -> []
                     ES5 -> commonjsPrefix) ++
                reexportChildren jsSyntax node ++
                case jsSyntax of
                    ES6 -> [ "export * from './module';" | not (isVirtual node) ]
                    ES5 -> [ "__export(require('./module'));" | not (isVirtual node) ]
        T.writeFileUtf8 (modDir </> "index.d.ts") (indexContent ES6)
        T.writeFileUtf8 (modDir </> "index.js") (indexContent ES5)
        processChildren modName node

    reexportChildren :: JSSyntax -> IndexTree -> [T.Text]
    reexportChildren jsSyntax = concatMap reexport . Map.keys . children
      where
        reexport name = case jsSyntax of
            ES6 -> [ "import * as " <> name <> " from './" <> name <> "';"
                   , "export { " <> name <>  " } ;"
                   ]
            ES5 -> [ "var " <> name <> " = require('./" <> name <> "');"
                   , "exports." <> name <> " = " <> name <> ";"
                   ]

data JSSyntax
    = ES6 -- ^ We use this for .d.ts files
    | ES5 -- ^ We generate ES5 JS with commonjs modules
          -- That matches what we used to generate by invoking
          -- the typescript compiler.

-- | Prefix for a commonjs module. This matches
-- what the typescript compiler would also emit.
commonjsPrefix :: [T.Text]
commonjsPrefix =
    [ "\"use strict\";"
    , "/* eslint-disable-next-line no-unused-vars */"
    , "function __export(m) {"
    , "/* eslint-disable-next-line no-prototype-builtins */"
    , "    for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];"
    , "}"
    , "Object.defineProperty(exports, \"__esModule\", { value: true });"
    ]
