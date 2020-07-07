-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE DerivingStrategies #-}
module TsCodeGenMain (main) where

import DA.Directory
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
import Data.Graph
import Data.Maybe
import Options.Applicative
import System.Directory
import System.Environment
import System.FilePath hiding ((<.>))
import System.Process
import System.Exit

import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import qualified DA.Daml.Project.Types as DATypes

-- Version of the the TypeScript compiler we're using.
tscVersion :: T.Text
tscVersion = "~3.8.3"

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
main = withProgName "daml codegen js" $ do
    opts@Options{..} <- customExecParser (prefs showHelpOnError) optionsParserInfo
    sdkVersionOrErr <- DATypes.parseVersion . T.pack . fromMaybe "0.0.0" <$> getSdkVersionMaybe
    sdkVersion <- case sdkVersionOrErr of
          Left _ -> fail "Invalid SDK version"
          Right v -> pure v
    pkgs <- readPackages optInputDars
    case mergePackageMap pkgs of
      Left err -> fail . T.unpack $ err
      Right pkgMap -> do
        dependencies <-
          forM (Map.toList pkgMap) $
            \(pkgId, (mbPkgName, pkg)) -> do
                 let id = unPackageId pkgId
                     pkgName = packageNameText pkgId mbPkgName
                 let pkgDesc = case mbPkgName of
                       Nothing -> id
                       Just pkgName -> unPackageName pkgName <> " (hash: " <> id <> ")"
                 T.putStrLn $ "Generating " <> pkgDesc
                 daml2js Daml2jsParams{..}
        buildPackages sdkVersion optScope optOutputDir dependencies

packageNameText :: PackageId -> Maybe PackageName -> T.Text
packageNameText pkgId mbPkgIdent = maybe (unPackageId pkgId) unPackageName mbPkgIdent

newtype Scope = Scope {unScope :: T.Text}
newtype Dependency = Dependency {unDependency :: T.Text} deriving (Eq, Ord)

data Daml2jsParams = Daml2jsParams
  { opts :: Options  -- cli args
  , pkgMap :: Map.Map PackageId (Maybe PackageName, Package)
  , pkgId :: PackageId
  , pkg :: Package
  , pkgName :: T.Text
  , sdkVersion :: SdkVersion
  }

-- Write the files for a single package.
daml2js :: Daml2jsParams -> IO (T.Text, [Dependency])
daml2js Daml2jsParams {..} = do
    let Options {..} = opts
        scopeDir = optOutputDir
          -- The directory into which we generate packages e.g. '/path/to/daml2js'.
        packageDir = scopeDir </> T.unpack pkgName
          -- The directory into which we write this package e.g. '/path/to/daml2js/davl-0.0.4'.
        packageSrcDir = packageDir </> "src"
          -- Where the source files of this package are written e.g. '/path/to/daml2js/davl-0.0.4/src'.
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
    pure (pkgName, dependencies)
    where
      -- Write the .ts file for a single DAML-LF module.
      writeModuleTs :: FilePath -> Scope -> Module -> IO (Maybe ModuleName, Set.Set Dependency)
      writeModuleTs packageSrcDir scope mod = do
        case genModule pkgMap scope pkgId mod of
          Nothing -> pure (Nothing, Set.empty)
          Just (modTxt, ds) -> do
            let outputFile = packageSrcDir </> joinPath (map T.unpack (unModuleName (moduleName mod))) </> "module.ts"
            createDirectoryIfMissing True (takeDirectory outputFile)
            T.writeFileUtf8 outputFile modTxt
            pure (Just (moduleName mod), ds)

-- Generate the .ts content for a single module.
genModule :: Map.Map PackageId (Maybe PackageName, Package) ->
     Scope -> PackageId -> Module -> Maybe (T.Text, Set.Set Dependency)
genModule pkgMap (Scope scope) curPkgId mod
  | null serDefs =
    Nothing -- If no serializable types, nothing to do.
  | otherwise =
    let (decls, refs) = unzip (map (genDataDef curPkgId mod tpls) serDefs)
        imports = (PRSelf, modName) `Set.delete` Set.unions refs
        (internalImports, externalImports) = splitImports imports
        rootPath = map (const "..") (unModuleName modName)
        modText = T.unlines $ intercalate [""] $ filter (not . null) $
          modHeader
          : map (externalImportDecl pkgMap) (Set.toList externalImports)
          : map (internalImportDecl rootPath) internalImports
          : map (map renderTsDecl) decls
        depends = Set.map (Dependency . pkgRefStr pkgMap) externalImports
   in Just (modText, depends)
  where
    modName = moduleName mod
    tpls = moduleTemplates mod
    serDefs = defDataTypes mod
    modHeader =
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
    internalImportDecl :: [T.Text] -> ModuleName -> T.Text
    internalImportDecl rootPath modName =
      "import * as " <> genModuleRef (PRSelf, modName) <> " from '" <>
        modPath (rootPath ++ unModuleName modName ++ ["module"]) <> "';"

    -- Calculate an import declaration for a module from another package.
    externalImportDecl :: Map.Map PackageId (Maybe PackageName, Package) ->
                      PackageId -> T.Text
    externalImportDecl pkgMap pkgId =
      "import * as " <> pkgVar pkgId <> " from '" <> scope <> "/" <> pkgRefStr pkgMap pkgId <> "';"

    -- Produce a package name for a package ref.
    pkgRefStr :: Map.Map PackageId (Maybe PackageName, Package) -> PackageId -> T.Text
    pkgRefStr pkgMap pkgId =
        case Map.lookup pkgId pkgMap of
          Nothing -> error "IMPOSSIBLE : package map malformed"
          Just (mbPkgName, _) -> packageNameText pkgId mbPkgName

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
        tyDecls = [d | d@DeclTypeDef{} <- decls]

-- | The typescript declarations we produce.
data TsDecl
    = DeclTemplateDef TemplateDef
    | DeclSerializableDef SerializableDef
    | DeclTypeDef TypeDef
    | DeclTemplateNamespace TemplateNamespace
    | DeclTemplateRegistration TemplateRegistration
    | DeclNamespace T.Text [TsDecl]
    -- ^ Note that we special-case some namespaces, e.g., the template namespace
    -- that always have fixed contents. This constructor is only used for the namespace
    -- for sums of products.

renderTsDecl :: TsDecl -> T.Text
renderTsDecl = \case
    DeclTemplateDef t -> renderTemplateDef t
    DeclSerializableDef t -> renderSerializableDef t
    DeclTypeDef t -> renderTypeDef t
    DeclTemplateNamespace t -> renderTemplateNamespace t
    DeclTemplateRegistration t -> renderTemplateRegistration t
    DeclNamespace t decls -> T.unlines $ concat
        [ [ "export namespace " <> t <> " {" ]
        , [ "  " <> l | d <- decls, l <- T.lines (renderTsDecl d) ]
        , [ "} //namespace " <> t ]
        ]


-- | Namespace containing type synonyms for Key, CreatedEvent, ArchivedEvent and Event
-- for the given template.
data TemplateNamespace = TemplateNamespace
  { tnsName :: T.Text
  , tnsMbKeyDef :: Maybe T.Text
  }

renderTemplateNamespace :: TemplateNamespace -> T.Text
renderTemplateNamespace TemplateNamespace{..} = T.unlines $ concat
    [ [ "export namespace " <> tnsName <> " {" ]
    , [ "  export type Key = " <> keyDef | Just keyDef <- [tnsMbKeyDef] ]
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
  [ "damlTypes.registerTemplate(" <> t <> ");" ]

data TemplateDef = TemplateDef
  { tplName :: T.Text
  , tplPkgId :: PackageId
  , tplModule :: ModuleName
  , tplDecoder :: Decoder
  , tplKeyDecoder :: Maybe Decoder
  -- ^ Nothing if we do not have a key.
  , tplChoices' :: [ChoiceDef]
  }

renderTemplateDef :: TemplateDef -> T.Text
renderTemplateDef TemplateDef{..} = T.unlines $ concat
    [ [ "export const " <> tplName <> ":"
      , "  damlTypes.Template<" <> tplName <> ", " <> keyTy <> ", '" <> templateId <> "'> & {"
      ]
    , [ "  " <> chcName' <> ": damlTypes.Choice<" <>
        tplName <> ", " <>
        chcArgTy <> ", " <>
        chcRetTy <> ", " <>
        keyTy <> ">;" | ChoiceDef{..} <- tplChoices' ]
    , [ "} = {"
      , "  templateId: '" <> templateId <> "',"
      , "  keyDecoder: " <> keyDec <> ","
      , "  decoder: () => " <> renderDecoder tplDecoder <> ","
      ]
    , concat
      [ [ "  " <> chcName' <> ": {"
        , "    template: () => " <> tplName <> ","
        , "    choiceName: '" <> chcName' <> "',"
        , "    argumentDecoder: " <> chcArgTy <> ".decoder,"
        , "    resultDecoder: () => " <> chcRetSerTy <> ".decoder(),"
        , "  },"
        ]
      | ChoiceDef{..} <- tplChoices'
      ]
    , [ "};" ]
    ]
  where (keyTy, keyDec) = case tplKeyDecoder of
            Nothing -> ("undefined", "() => " <> renderDecoder (DecoderConstant ConstantUndefined))
            Just d -> (tplName <> ".Key", "() => " <> renderDecoder d)
        templateId =
            unPackageId tplPkgId <> ":" <>
            T.intercalate "." (unModuleName tplModule) <> ":" <>
            tplName

data ChoiceDef = ChoiceDef
  { chcName' :: T.Text
  , chcArgTy :: T.Text
  , chcRetTy :: T.Text
  -- ^ Return type in typescript, e.g., `{}` for `()`
  , chcRetSerTy :: T.Text
  -- ^ Type that has the serializability definition, e.g., `damlTypes.Unit` for `()`.
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

renderSerializableDef :: SerializableDef -> T.Text
renderSerializableDef SerializableDef{..}
  | null serParams = T.unlines $ concat
      [ [ "export const " <> serName <> ":"
        , "  damlTypes.Serializable<" <> serName <> "> & {"
        ]
      , [ "  " <> n <> ": damlTypes.Serializable<" <> serName <.> n <> ">;" | (n, _) <- serNestedDecoders ]
      , [ "  } "
        ]
      , [ "& { readonly keys: " <> serName <> "[] } & { readonly [e in " <> serName <> "]: e }" | notNull serKeys ]
      , [ "  = ({" ]
      , [ "  " <> k <> ":" <> "'" <> k <> "'," | k <- serKeys ]
      , [ "  keys: [" <> T.concat (map (\s -> "'" <> s <> "',") serKeys) <> "]," | notNull serKeys ]
      , [ "  decoder: () => " <> renderDecoder serDecoder <> ","
        ]
      , concat $
        [ [ "  " <> n <> ":({"
          , "    decoder: () => " <> renderDecoder d <> ","
          , "  }),"
          ]
        | (n, d) <- serNestedDecoders
        ]
      , [ "});" ]
      ]
  | otherwise = assert (null serKeys) $ T.unlines $
  -- If we have type parameters, the serializable definition is
  -- a function and we generate extra properties on that function
  -- for each nested decoder.
      [ "export const " <> serName <> " = " <> tyArgs <> ":"
      , "  damlTypes.Serializable<" <> serName <> tyParams <> "> => ({"
      , "  decoder: () => " <> renderDecoder serDecoder <> ","
      , "});"
      ] <> concat
      [ [ serName <.> n <> " = " <> tyArgs <> ":"
        , "  damlTypes.Serializable<" <> serName <.> n <> tyParams <> "> => ({"
        , "  decoder: () => " <> renderDecoder d <> ","
        , "});"
        ]
      | (n, d) <- serNestedDecoders
      ]
  where tyParams = "<" <> T.intercalate ", " serParams <> ">"
        tyArgs = tyParams <> "(" <> T.intercalate ", " (map (\name -> name <> ": damlTypes.Serializable<" <> name <> ">") serParams) <> ")"


data Decoder
    = DecoderOneOf T.Text [Decoder]
    | DecoderObject [(T.Text, Decoder)]
    | DecoderConstant DecoderConstant
    | DecoderRef T.Text -- ^ Reference to an object with a .decoder() method
    | DecoderLazy Decoder

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
    DecoderOneOf constr branches ->
        "jtv.oneOf<" <> constr <> ">(" <>
        T.concat (map (\b -> renderDecoder b <> ",") branches) <>
        ")"
    DecoderObject fields ->
        "jtv.object({" <>
        T.concat (map (\(name, d) -> name <> ":" <> renderDecoder d <> ",") fields) <>
        "})"
    DecoderConstant c -> "jtv.constant(" <> renderDecoderConstant c <> ")"
    DecoderRef n -> n <> ".decoder()"
    DecoderLazy d -> "jtv.lazy(() => " <> renderDecoder d <> ")"

data TypeDef
    = UnionDef T.Text [T.Text] [(T.Text, T.Text)]
    | ObjectDef T.Text [T.Text] [(T.Text, T.Text)]
    | EnumDef T.Text [T.Text] [T.Text]

renderTypeDef :: TypeDef -> T.Text
renderTypeDef = \case
    UnionDef t args bs -> T.unlines $ concat
        [ [ "export type " <> ty t args <> " =" ]
        , [ "  |  { tag: '" <> k <> "'; value: " <> v <> " }" | (k, v) <- bs ]
        ]
    ObjectDef t args fs -> T.unlines $ concat
        [ [ "export type " <> ty t args <> " = {" ]
        , [ "  " <> k <> ": " <> v <> ";" | (k, v) <- fs ]
        , [ "}" ]
        ]
    EnumDef t args fs -> T.unlines $ concat
        [ [ "export type " <> ty t args <> " =" ]
        , [ "  | '" <> f <> "'" | f <- fs ]
        ]
  where ty t args = t <> "<" <> T.intercalate ", " args <> ">"

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
                 , serDecoder = DecoderOneOf conName [DecoderConstant (ConstantRef (conName <.> cons)) | cons <- cs]
                 , serNestedDecoders = []
                 }
        DataRecord fields ->
            let (fieldNames, fieldTypesLf) = unzip [(unFieldName x, t) | (x, t) <- fields]
                fieldSers = map (snd . genType (moduleName mod)) fieldTypesLf
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
        let (_, ser) = genType (moduleName mod) t
        in DecoderObject
             [ ("tag", DecoderConstant (ConstantString cons))
             , ("value", DecoderLazy (DecoderRef ser))
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
                | (VariantConName cons, t) <- bs, let (typ, _) = genType (moduleName mod) t
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
                [ (n, fst (genType (moduleName mod) ty)) | (FieldName n, ty) <- fields ]

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
                (_, fieldSers) = unzip (map (genType (moduleName mod)) fieldTypesLf)
                fieldRefs = map (Set.setOf typeModuleRef . snd) fields
                typeDesc = genTypeDef conName mod def
            in
            case NM.lookup (dataTypeCon def) tpls of
                Nothing -> ([DeclTypeDef typeDesc, DeclSerializableDef $ genSerializableDef curPkgId conName mod def], Set.unions fieldRefs)
                Just tpl ->
                    let (chcs, chcRefs) = unzip
                            [(ChoiceDef (unChoiceName (chcName chc)) t rtyp rser, Set.union argRefs retRefs)
                            | chc <- NM.toList (tplChoices tpl)
                            , let tLf = snd (chcArgBinder chc)
                            , let rLf = chcReturnType chc
                            , let (t, _) = genType (moduleName mod) tLf
                            , let (rtyp, rser) = genType (moduleName mod) rLf
                            , let argRefs = Set.setOf typeModuleRef tLf
                            , let retRefs = Set.setOf typeModuleRef rLf
                            ]
                        (keyDecoder, keyRefs) = case tplKey tpl of
                            Nothing -> (Nothing, Set.empty)
                            Just key ->
                                let keyType = tplKeyType key
                                in
                                (Just (DecoderRef $ snd (genType (moduleName mod) keyType)), Set.setOf typeModuleRef keyType)
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
                          , tnsMbKeyDef = fst . genType (moduleName mod) . tplKeyType <$> tplKey tpl
                          }
                        registrations = TemplateRegistration conName
                        refs = Set.unions (fieldRefs ++ keyRefs : chcRefs)
                    in
                    ([DeclTypeDef typeDesc, DeclTemplateDef dict, DeclTemplateNamespace associatedTypes, DeclTemplateRegistration registrations], refs)

infixr 6 <.> -- This is the same fixity as '<>'.
(<.>) :: T.Text -> T.Text -> T.Text
(<.>) u v = u <> "." <> v

genType :: ModuleName -> Type -> (T.Text, T.Text)
genType curModName = go
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

genTypeCon :: ModuleName -> Qualified TypeConName -> (T.Text, T.Text)
genTypeCon curModName (Qualified pkgRef modName conParts) =
    case unTypeConName conParts of
        [] -> error "IMPOSSIBLE: empty type constructor name"
        _: _: _: _ -> error "TODO(MH): multi-part type constructor names"
        [c1 ,c2]
          | modRef == (PRSelf, curModName) -> dupe $ c1 <.> c2
          | otherwise -> dupe $ genModuleRef modRef <> c1 <.> c2
        [conName]
          | modRef == (PRSelf, curModName) -> dupe conName
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
        , "description" .= ("Generated by `daml codegen js` from DAML SDK " <> versionToText sdkVersion)
        , "dependencies" .= packageJsonDependencies sdkVersion scope dependencies
        ]
  in
  BSL.writeFile (packageDir </> "package.json") (encodePretty packageJson)

buildPackages :: SdkVersion -> Scope -> FilePath -> [(T.Text, [Dependency])] -> IO ()
buildPackages sdkVersion optScope optOutputDir dependencies = do
  let (g, nodeFromVertex) = graphFromEdges'
        (map (\(a, ds) -> (a, a, map unDependency ds)) dependencies)
      pkgs = map (T.unpack . fst3 . nodeFromVertex) $ reverse (topSort g)
  withCurrentDirectory optOutputDir $ do
    BSL.writeFile "package.json" $ encodePretty packageJson
    yarn ["install"]
    createDirectoryIfMissing True $ "node_modules" </> scope
    mapM_ build pkgs
    removeFile "package.json" -- Any subsequent runs will regenerate it.
    -- We don't remove 'node_modules' : subsequent runs can benefit from caching.
  where
    packageJson :: Value
    packageJson = object
      [ "private" .= True
      , "name" .= ("daml2js" :: T.Text)
      , "version" .= version
      , "license" .= ("UNLICENSED" :: T.Text)
      , "dependencies" .= packageJsonDependencies sdkVersion optScope []
      , "devDependencies" .= object
          [ "typescript" .= tscVersion
          ]
      ]

    scope = T.unpack $ unScope optScope
    version = versionToText sdkVersion

    build :: String -> IO ()
    build pkg = do
      putStrLn $ "Building " <> pkg
      yarn ["run", "tsc", "--project", pkg </> "tsconfig.json"]
      copyDirectory pkg $ "node_modules" </> scope </> pkg

    yarn :: [String] -> IO ()
    yarn args = do
      -- We need to use `shell` instead of `proc` since at least in some cases
      -- `yarn` is called `yarn.cmd` which will not be picked up by `proc`.
      -- We could hardcode `yarn.cmd` on Windows but that seems rather fragile.
      (exitCode, out, err) <- readCreateProcessWithExitCode (shell $ unwords $ "yarn" : args) ""
      unless (exitCode == ExitSuccess) $ do
        putStrLn $ "Failure: \"yarn " <> unwords args <> "\" exited with " <> show exitCode
        -- User reports suggest that yarn writes its errors to stdout
        -- rather than stderr. Accordingly, we capture both.
        putStrLn out
        putStrLn err
        exitFailure

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
  T.writeFileUtf8 (srcDir </> "index.ts") $ T.unlines $
    reexportChildren root ++
    [ "export const packageId = '" <> unPackageId pkgId <> "';" ]
  processChildren (ModuleName []) root
  where
    processChildren :: ModuleName -> IndexTree -> IO ()
    processChildren parentModName parent =
      forM_ (Map.toList (children parent)) $ \(name, node) -> do
        let modName = ModuleName (unModuleName parentModName ++ [name])
        let modDir = srcDir </> joinPath (map T.unpack (unModuleName modName))
        createDirectoryIfMissing True modDir
        T.writeFileUtf8 (modDir </> "index.ts") $ T.unlines $
          reexportChildren node ++
          [ "export * from './module';" | not (isVirtual node) ]
        processChildren modName node

    reexportChildren :: IndexTree -> [T.Text]
    reexportChildren = concatMap reexport . Map.keys . children
      where
        reexport name =
          [ "import * as " <> name <> " from './" <> name <> "';"
          , "export import " <> name <> " = " <> name <> ";"
          ]
