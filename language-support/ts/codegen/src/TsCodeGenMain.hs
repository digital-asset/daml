-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE DerivingStrategies #-}
module TsCodeGenMain (main) where

import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Daml.LF.Reader as DAR
import qualified DA.Pretty
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import qualified Data.HashMap.Strict as HMS
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified Data.Set.Lens as Set
import qualified Data.Text.Extended as T
import qualified Data.Text.IO as T
import qualified "zip-archive" Codec.Archive.Zip as Zip
import qualified Data.Map as Map
import Data.Aeson hiding (Options)
import Data.Aeson.Encode.Pretty
import Data.Hashable

import Control.Exception
import Control.Monad.Extra
import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Optics
import Data.Tuple.Extra
import Data.List.Extra
import Data.Graph
import Data.Maybe
import Data.Bifoldable
import Options.Applicative
import System.IO.Error
import System.Directory
import System.FilePath hiding ((<.>), (</>))
import System.FilePath.Posix((</>)) -- Make sure we generate / on all platforms.
import qualified System.FilePath as FP

import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import qualified DA.Daml.Project.Types as DATypes

-- Referenced from 'writePackageJson'. Lifted here for easy
-- maintenance.
data ConfigConsts = ConfigConsts
  { pkgDependencies :: HMS.HashMap NpmPackageName NpmPackageVersion
  , pkgDevDependencies :: HMS.HashMap NpmPackageName NpmPackageVersion
  , pkgScripts :: HMS.HashMap ScriptName Script
  }
configConsts :: T.Text -> ConfigConsts
configConsts sdkVersion = ConfigConsts
  { pkgDependencies = HMS.fromList
      [ (NpmPackageName "@mojotech/json-type-validation", NpmPackageVersion "^3.1.0")
      , (NpmPackageName "@daml/types", NpmPackageVersion sdkVersion)
      ]
  , pkgDevDependencies = HMS.fromList
      [ (NpmPackageName "@typescript-eslint/eslint-plugin", NpmPackageVersion "2.11.0")
      , (NpmPackageName "@typescript-eslint/parser", NpmPackageVersion "2.11.0")
      , (NpmPackageName "eslint", NpmPackageVersion "^6.7.2")
      , (NpmPackageName "typescript", NpmPackageVersion "~3.7.3")
      ]
  , pkgScripts = HMS.fromList
      [ (ScriptName "build", Script "tsc --build")
      , (ScriptName "lint", Script "eslint --ext .ts --max-warnings 0 src/")
      ]
  }
newtype NpmPackageName = NpmPackageName {unNpmPackageName :: T.Text}
  deriving stock (Eq, Show)
  deriving newtype (Hashable, FromJSON, ToJSON, ToJSONKey)
newtype NpmPackageVersion = NpmPackageVersion {unNpmPackageVersion :: T.Text}
  deriving stock (Eq, Show)
  deriving newtype (Hashable, FromJSON, ToJSON)
newtype ScriptName = ScriptName {unScriptName :: T.Text}
  deriving stock (Eq, Show)
  deriving newtype (Hashable, FromJSON, ToJSON, ToJSONKey)
newtype Script = Script {unScript :: T.Text}
  deriving stock (Eq, Show)
  deriving newtype (Hashable, FromJSON, ToJSON)

data Options = Options
    { optInputDars :: [FilePath]
    , optOutputDir :: FilePath
    , optInputPackageJson :: FilePath
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
    <*> strOption
        (  short 'p'
        <> metavar "PACKAGE-JSON"
        <> value "package.json"
        <> help "Path to a 'package.json' to update (or create if missing)"
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
    opts@Options{..} <- execParser optionsParserInfo
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
                     asName = if pkgName == id then "itself" else pkgName
                 T.putStrLn $ "Generating " <> id <> " as " <> asName
                 daml2ts Daml2TsParams{..}
        setupWorkspace optInputPackageJson optOutputDir dependencies

packageNameText :: PackageId -> Maybe PackageName -> T.Text
packageNameText pkgId mbPkgIdent = maybe (unPackageId pkgId) unPackageName mbPkgIdent

newtype Scope = Scope T.Text
newtype Dependency = Dependency {unDependency :: T.Text}  deriving (Eq, Ord)

data Daml2TsParams = Daml2TsParams
  { opts :: Options  -- cli args
  , pkgMap :: Map.Map PackageId (Maybe PackageName, Package)
  , pkgId :: PackageId
  , pkg :: Package
  , pkgName :: T.Text
  , sdkVersion :: SdkVersion
  }

-- Write the files for a single package.
daml2ts :: Daml2TsParams -> IO (T.Text, [Dependency])
daml2ts Daml2TsParams {..} = do
    let Options {..} = opts
        scopeDir = optOutputDir
          -- The directory into which we generate packages e.g. '/path/to/daml2ts'.
        packageDir = scopeDir </> T.unpack pkgName
          -- The directory into which we write this package e.g. '/path/to/daml2ts/davl-0.0.4'.
        packageSrcDir = packageDir </> "src"
          -- Where the source files of this package are written e.g. '/path/to/daml2ts/davl-0.0.4/src'.
        scope = optScope
          -- The scope e.g. '@daml.js'.
          -- We use this, for example, when generating import declarations e.g.
          --   import * as pkgd14e08_DA_Internal_Template from '@daml.js/d14e08/lib/DA/Internal/Template';
    createDirectoryIfMissing True packageSrcDir
    let modules = NM.toList (packageModules pkg)
    -- Write .ts files for the package and harvest references to
    -- foreign packages as we do.
    dependencies <- nubOrd . concat <$> mapM (writeModuleTs packageSrcDir scope) (NM.toList (packageModules pkg))
    -- Now write package metadata.
    writePackageIdTs packageSrcDir pkgId
    writeIndexTs packageSrcDir modules
    writeTsConfig packageDir
    writeEsLintConfig packageDir
    writePackageJson packageDir sdkVersion scope dependencies
    pure (pkgName, dependencies)
    where
      -- Write the .ts file for a single DAML-LF module.
      writeModuleTs :: FilePath -> Scope -> Module -> IO [Dependency]
      writeModuleTs packageSrcDir scope mod = do
        case genModule pkgMap scope pkgId mod of
          Nothing -> pure []
          Just (modTxt, ds) -> do
            let outputFile = packageSrcDir </> T.unpack (modPath (unModuleName (moduleName mod))) FP.<.> "ts"
            createDirectoryIfMissing True (takeDirectory outputFile)
            T.writeFileUtf8 outputFile modTxt
            pure ds

-- Generate the .ts content for a single module.
genModule :: Map.Map PackageId (Maybe PackageName, Package) ->
     Scope -> PackageId -> Module -> Maybe (T.Text, [Dependency])
genModule pkgMap (Scope scope) curPkgId mod
  | null serDefs =
    Nothing -- If no serializable types, nothing to do.
  | otherwise =
    let (defSers, refs) = unzip (map (genDataDef curPkgId mod tpls) serDefs)
        imports = [ importDecl pkgMap modRef rootPath
                  | modRef@(pkgRef, _) <- modRefs refs
                  , let rootPath = pkgRootPath modName pkgRef ]
        defs = map biconcat defSers
        modText = T.unlines $ intercalate [""] $ filter (not . null) $ modHeader : imports : defs
        depends = [ Dependency $ pkgRefStr pkgMap pkgRef
                  | (pkgRef, _) <- modRefs refs, pkgRef /= PRSelf ]
   in Just (modText, depends)
  where
    modName = moduleName mod
    tpls = moduleTemplates mod
    serDefs = defDataTypes mod
    modRefs refs = Set.toList ((PRSelf, modName) `Set.delete` Set.unions refs)
    modHeader =
      [ "// Generated from " <> modPath (unModuleName modName) <> ".daml"
      , "/* eslint-disable @typescript-eslint/camelcase */"
      , "/* eslint-disable @typescript-eslint/no-use-before-define */"
      , "import * as jtv from '@mojotech/json-type-validation';"
      , "import * as daml from '@daml/types';"
      ]

    -- Calculate an import declaration.
    importDecl :: Map.Map PackageId (Maybe PackageName, Package) ->
                      (PackageRef, ModuleName) -> T.Text -> T.Text
    importDecl pkgMap modRef@(pkgRef, modName) rootPath =
      "import * as " <>  genModuleRef modRef <> " from '" <>
      modPath ((rootPath : pkgRefStr pkgMap pkgRef : ["lib" | pkgRef /= PRSelf]) ++ unModuleName modName) <>
      "';"

    -- Produce a package name for a package ref.
    pkgRefStr :: Map.Map PackageId (Maybe PackageName, Package) -> PackageRef -> T.Text
    pkgRefStr pkgMap = \case
      PRSelf -> ""
      PRImport pkgId ->
        case Map.lookup pkgId pkgMap of
          Nothing -> error "IMPOSSIBLE : package map malformed"
          Just (mbPkgName, _) -> packageNameText pkgId mbPkgName

    -- Calculate the root part of a package ref string. For foreign
    -- imports that's something like '@daml2ts'. For self refences
    -- something like '../../'.
    pkgRootPath :: ModuleName -> PackageRef -> T.Text
    pkgRootPath modName pkgRef =
      case pkgRef of
        PRSelf ->
          if lenModName == 1
            then "."
            else modPath $ replicate (lenModName - 1) (T.pack "..")
        PRImport _ -> scope
      where lenModName = length (unModuleName modName)

defDataTypes :: Module -> [DefDataType]
defDataTypes mod = filter (getIsSerializable . dataSerializable) (NM.toList (moduleDataTypes mod))

genDataDef :: PackageId -> Module -> NM.NameMap Template -> DefDataType -> (([T.Text], [T.Text]), Set.Set ModuleRef)
genDataDef curPkgId mod tpls def = case unTypeConName (dataTypeCon def) of
    [] -> error "IMPOSSIBLE: empty type constructor name"
    _: _: _: _ -> error "IMPOSSIBLE: multi-part type constructor of more than two names"

    [conName] -> genDefDataType curPkgId conName mod tpls def
    [c1, c2] -> ((makeNamespace $ map ("  " <>) typs, []), refs)
      where
        ((typs, _), refs) = genDefDataType curPkgId c2 mod tpls def
        makeNamespace stuff =
          [ "// eslint-disable-next-line @typescript-eslint/no-namespace"
          , "export namespace " <> c1 <> " {"] ++ stuff ++ ["} //namespace " <> c1]

genDefDataType :: PackageId -> T.Text -> Module -> NM.NameMap Template -> DefDataType -> (([T.Text], [T.Text]), Set.Set ModuleRef)
genDefDataType curPkgId conName mod tpls def =
    case dataCons def of
        DataVariant bs ->
          let
            (typs, sers) = unzip $ map genBranch bs
            typeDesc = makeType ([""] ++ typs)
            typ = conName <> typeParams -- Type of the variant.
            serDesc =
              if not $ null paramNames -- Polymorphic type.
              then -- Companion function.
                let
                  -- Any associated serializers.
                  assocSers = map (\(n, d) -> serFromDef id n d) assocDefDataTypes
                  -- The variant deserializer.
                  function = onLast (<> ";") (makeSer ( ["() => jtv.oneOf<" <> typ <> ">("] ++ sers ++ [")"]));
                  props = -- Fix the first and last line of each serializer.
                    concatMap (onHead (fromJust . T.stripPrefix (T.pack "export const ")) . onLast (<> ";")) assocSers
                  -- The complete definition of the companion function.
                  in function ++ props
              else -- Companion object.
                let
                  assocNames = map fst assocDefDataTypes
                  -- Any associated serializers, dropping the first line
                  -- of each.
                  assocSers = map (\(n, d) -> (n, serFromDef (drop 1) n d)) assocDefDataTypes
                  -- Type of the companion object.
                  typ' = "daml.Serializable<" <> conName <> "> & {\n" <>
                    T.concat (map (\n -> "    " <> n <> ": daml.Serializable<" <> (conName <.> n) <> ">;\n") assocNames) <>
                    "  }"
                  -- Body of the companion object.
                  body = map ("  " <>) $
                    -- The variant deserializer.
                    ["decoder: () => jtv.oneOf<" <> typ <> ">("] ++  sers ++ ["),"] ++
                    -- Remember how we dropped the first line of each
                    -- associated serializer above? This replaces them.
                    concatMap (\(n, ser) -> n <> ": ({" : onLast (<> ",") ser) assocSers
                  -- The complete definition of the companion object.
                  in ["export const " <> conName <> ":\n  " <> typ' <> " = ({"] ++ body ++ ["});"]
            in ((typeDesc, serDesc), Set.unions $ map (Set.setOf typeModuleRef . snd) bs)
        DataEnum enumCons ->
          let cs = map unVariantConName enumCons
              typeDesc = "" : ["  | '" <> cons <> "'" | cons <- cs]
              -- The complete definition of the companion object.
              serDesc =
                ["export const " <> conName <> ": daml.Serializable<" <> conName <> "> " <>
                 "& { readonly keys: " <> conName <> "[] } & { readonly [e in " <> conName <> "]: e } = {"] ++
                ["  " <> cons <> ": '" <> cons <> "'," | cons <- cs] ++
                ["  keys: [" <> T.concat ["'" <> cons <> "'," | cons <- cs] <> "],"] ++
                ["  decoder: () => jtv.oneOf<" <> conName <> ">" <> "("] ++
                ["      jtv.constant(" <> conName <> "." <> cons <> ")," | cons <- cs] ++
                ["  ),"] ++
                ["};"]
          in
          ((makeType typeDesc, serDesc), Set.empty)
        DataRecord fields ->
            let (fieldNames, fieldTypesLf) = unzip [(unFieldName x, t) | (x, t) <- fields]
                (fieldTypesTs, fieldSers) = unzip (map (genType (moduleName mod)) fieldTypesLf)
                fieldRefs = map (Set.setOf typeModuleRef . snd) fields
                typeDesc =
                    ["{"] ++
                    ["  " <> x <> ": " <> t <> ";" | (x, t) <- zip fieldNames fieldTypesTs] ++
                    ["}"]
                serDesc =
                    ["() => jtv.object({"] ++
                    ["  " <> x <> ": " <> ser <> ".decoder()," | (x, ser) <- zip fieldNames fieldSers] ++
                    ["})"]
            in
            case NM.lookup (dataTypeCon def) tpls of
                Nothing -> ((makeType typeDesc, makeSer serDesc), Set.unions fieldRefs)
                Just tpl ->
                    let (chcs, chcRefs) = unzip
                            [((unChoiceName (chcName chc), t, rtyp, rser), Set.union argRefs retRefs)
                            | chc <- NM.toList (tplChoices tpl)
                            , let tLf = snd (chcArgBinder chc)
                            , let rLf = chcReturnType chc
                            , let (t, _) = genType (moduleName mod) tLf
                            , let (rtyp, rser) = genType (moduleName mod) rLf
                            , let argRefs = Set.setOf typeModuleRef tLf
                            , let retRefs = Set.setOf typeModuleRef rLf
                            ]
                        (keyTypeTs, keySer, keyRefs) = case tplKey tpl of
                            Nothing -> ("undefined", "() => jtv.constant(undefined)", Set.empty)
                            Just key ->
                                let keyType = tplKeyType key
                                in
                                (conName <.> "Key", "() => " <> snd (genType (moduleName mod) keyType) <> ".decoder()", Set.setOf typeModuleRef keyType)
                        templateId = unPackageId curPkgId <> ":" <> T.intercalate "." (unModuleName (moduleName mod)) <> ":" <> conName
                        dict =
                            ["export const " <> conName <> ": daml.Template<" <> conName <> ", " <> keyTypeTs <> ", '" <> templateId <> "'> & {"] ++
                            ["  " <> x <> ": daml.Choice<" <> conName <> ", " <> t <> ", " <> rtyp <> ", " <> keyTypeTs <> ">;" | (x, t, rtyp, _) <- chcs] ++
                            ["} = {"
                            ] ++
                            ["  templateId: '" <> templateId <> "',"
                            ,"  keyDecoder: " <> keySer <> ","
                            ] ++
                            map ("  " <>) (onLast (<> ",") (onHead ("decoder: " <>) serDesc)) ++
                            concat
                            [ ["  " <> x <> ": {"
                              ,"    template: () => " <> conName <> ","
                              ,"    choiceName: '" <> x <> "',"
                              ,"    argumentDecoder: " <> t <> ".decoder,"
                              -- We'd write,
                              --   "   resultDecoder: " <> rser <> ".decoder"
                              -- here but, consider the following scenario:
                              --   export const Person: daml.Template<Person>...
                              --    = {  ...
                              --         Birthday: { resultDecoder: daml.ContractId(Person).decoder, ... }
                              --         ...
                              --      }
                              -- This gives rise to "error TS2454: Variable 'Person' is used before being assigned."
                              ,"    resultDecoder: () => " <> rser <> ".decoder()," -- Eta-conversion provides an escape hatch.
                              ,"  },"
                              ]
                            | (x, t, _rtyp, rser) <- chcs
                            ] ++
                            ["};"]
                        associatedTypes =
                          maybe [] (\key ->
                              [ "// eslint-disable-next-line @typescript-eslint/no-namespace"
                              , "export namespace " <> conName <> " {"] ++
                              ["  export type Key = " <> fst (genType (moduleName mod) (tplKeyType key)) <> ""] ++
                              ["}"]) (tplKey tpl)
                        registrations =
                            ["daml.registerTemplate(" <> conName <> ");"]
                        refs = Set.unions (fieldRefs ++ keyRefs : chcRefs)
                    in
                    ((makeType typeDesc, dict ++ associatedTypes ++ registrations), refs)
      where
        paramNames = map (unTypeVarName . fst) (dataParams def)
        typeParams
          | null paramNames = ""
          | otherwise = "<" <> T.intercalate ", " paramNames <> ">"
        serParam paramName = paramName <> ": daml.Serializable<" <> paramName <> ">"
        serHeader
          | null paramNames = ": daml.Serializable<" <> conName <> "> ="
          | otherwise = " = " <> typeParams <> "(" <> T.intercalate ", " (map serParam paramNames) <> "): daml.Serializable<" <> conName <> typeParams <> "> =>"
        makeType = onHead (\x -> "export type " <> conName <> typeParams <> " = " <> x)
        makeSer serDesc =
            ["export const " <> conName <> serHeader <> " ({"] ++
            map ("  " <>) (onLast (<> ",") (onHead ("decoder: " <>) serDesc)) ++
            ["})"]
        genBranch (VariantConName cons, t) =
          let (typ, ser) = genType (moduleName mod) t in
          ( "  |  { tag: '" <> cons <> "'; value: " <> typ <> " }"
          , "  jtv.object({tag: jtv.constant('" <> cons <> "'), value: jtv.lazy(() => " <> ser <> ".decoder())}),"
          )
        -- A type such as
        --   data Q = C { x: Int, y: Text }| G { z: Bool }
        -- has a DAML-LF representation like,
        --   record Q.C = { x: Int, y: String }
        --   record Q.G = { z: Bool }
        --   variant Q = C Q.C | G Q.G
        -- This constant is the definitions of 'Q.C' and 'Q.G' given
        -- 'Q'.
        assocDefDataTypes =
          [(sub, def) | def <- defDataTypes mod
            , [sup, sub] <- [unTypeConName (dataTypeCon def)], sup == conName]
        -- Extract the serialization code associated with a data type
        -- definition.
        serFromDef f c2 = f . snd . fst . genDefDataType curPkgId (conName <.> c2) mod tpls

infixr 6 <.> -- This is the same fixity as '<>'.
(<.>) :: T.Text -> T.Text -> T.Text
(<.>) u v = u <> "." <> v

genType :: ModuleName -> Type -> (T.Text, T.Text)
genType curModName = go
  where
    go = \case
        TVar v -> dupe (unTypeVarName v)
        TUnit -> ("{}", "daml.Unit")
        TBool -> ("boolean", "daml.Bool")
        TInt64 -> dupe "daml.Int"
        TDecimal -> dupe "daml.Decimal"
        TNumeric (TNat n) -> (
            "daml.Numeric"
          , "daml.Numeric(" <> T.pack (show (fromTypeLevelNat n :: Integer)) <> ")"
          )
        TText -> ("string", "daml.Text")
        TTimestamp -> dupe "daml.Time"
        TParty -> dupe "daml.Party"
        TDate -> dupe "daml.Date"
        TList t ->
            let (t', ser) = go t
            in
            (t' <> "[]", "daml.List(" <> ser <> ")")
        TOptional t ->
            let (t', ser) = go t
            in
            ("daml.Optional<" <> t' <> ">", "daml.Optional(" <> ser <> ")")
        TTextMap t  ->
            let (t', ser) = go t
            in
            ("{ [key: string]: " <> t' <> " }", "daml.TextMap(" <> ser <> ")")
        TUpdate _ -> error "IMPOSSIBLE: Update not serializable"
        TScenario _ -> error "IMPOSSIBLE: Scenario not serializable"
        TContractId t ->
            let (t', ser) = go t
            in
            ("daml.ContractId<" <> t' <> ">", "daml.ContractId(" <> ser <> ")")
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

genModuleRef :: ModuleRef -> T.Text
genModuleRef (pkgRef, modName) = case pkgRef of
    PRSelf -> modVar "" name
    PRImport pkgId -> modVar ("pkg" <> unPackageId pkgId <> "_") name
  where
    name = unModuleName modName

-- Calculate a variable name from a module name e.g. 'modVar "__"
-- ["A", "B"]' is '__A_B'.
modVar :: T.Text -> [T.Text] -> T.Text
modVar prefix parts = prefix <> T.intercalate "_" parts

-- Calculate a filepath from a module name e.g. 'modPath [".", "A",
-- "B"]' is "./A/B".
modPath :: [T.Text] -> T.Text
modPath parts = T.intercalate "/" parts

onHead :: (a -> a) -> [a] -> [a]
onHead f = \case
    [] -> []
    x : xs -> f x : xs

onLast :: (a -> a) -> [a] -> [a]
onLast f = \case
    [] -> []
    [l] -> [f l]
    x : xs -> x : onLast f xs

writePackageIdTs :: FilePath -> PackageId -> IO ()
writePackageIdTs dir pkgId =
    T.writeFileUtf8 (dir </> "packageId.ts") $ T.unlines
      ["export default '" <> unPackageId pkgId <> "';"]

writeIndexTs :: FilePath -> [Module] -> IO ()
writeIndexTs packageSrcDir modules =
  T.writeFileUtf8 (packageSrcDir </> "index.ts") (T.unlines lines)
  where
    lines :: [T.Text]
    lines = header <> concatMap entry modules <> footer

    header :: [T.Text]
    header =
      [ "import __packageId from \"./packageId\""
      , "/* eslint-disable @typescript-eslint/no-namespace */"
      , "namespace __All {"
      , "  export const packageId = __packageId;"
      , "}"
      ]

    footer :: [T.Text]
    footer = [ "export default __All;" ]

    entry :: Module -> [T.Text]
    entry mod
      | null $ defDataTypes mod = []
      | otherwise = importDecl var path <> exportDecl var name
      where
        name = unModuleName (moduleName mod)
        var = modVar "__" name
        path = "\"" <> modPath ("." : name) <> "\""

    importDecl :: T.Text -> T.Text -> [T.Text]
    importDecl var path =
      [ "/* eslint-disable @typescript-eslint/camelcase */"
      , "import * as " <> var <> " from "  <> path <> ";"
      ]

    exportDecl :: T.Text -> [T.Text] -> [T.Text]
    exportDecl var name =
      [ "/* eslint-disable @typescript-eslint/no-namespace */"
      , "namespace __All {"
      ] <>  go 2 var name <>
      ["}"]
      where
        spaces i = T.pack $ replicate i ' '

        go :: Int -> T.Text -> [T.Text] -> [T.Text]
        go indent var [m] = let ws = spaces indent in
          [ ws <> "/* eslint-disable @typescript-eslint/no-unused-vars */"
          , ws <> "export import " <> m <> " = " <> var <> ";"
          ]
        go indent var (m : parts) = let ws = spaces indent in
          [ ws <> "/* eslint-disable @typescript-eslint/no-namespace */"
          , ws <> "export namespace " <> m <> " {"
          ] <>
          go (indent + 2) var parts <>
          [ ws <> "}" ]
        go _ _ [] = []

writeTsConfig :: FilePath -> IO ()
writeTsConfig dir =
  BSL.writeFile (dir </> "tsconfig.json") $ encodePretty tsConfig
  where
    tsConfig :: Value
    tsConfig = object
      [ "compilerOptions" .= object
        [ "target" .= ("es5" :: T.Text)
        , "lib" .= (["dom", "es2015"] :: [T.Text])
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

writeEsLintConfig :: FilePath -> IO ()
writeEsLintConfig dir =
  BSL.writeFile (dir </> ".eslintrc.json") $ encodePretty esLintConfig
  where
    esLintConfig :: Value
    esLintConfig = object
      [ "parser" .= ("@typescript-eslint/parser" :: T.Text)
      , "parserOptions" .= object [("project", "./tsconfig.json")]
      , "plugins" .= (["@typescript-eslint"] :: [T.Text])
      , "extends" .= (
          [ "eslint:recommended"
          , "plugin:@typescript-eslint/eslint-recommended"
          , "plugin:@typescript-eslint/recommended"
          , "plugin:@typescript-eslint/recommended-requiring-type-checking"
          ] :: [T.Text])
      , "rules" .= object
          [ ("@typescript-eslint/explicit-function-return-type", "off")
          , ("@typescript-eslint/no-inferrable-types", "off")
          ]
      ]

writePackageJson :: FilePath -> SdkVersion -> Scope -> [Dependency] -> IO ()
writePackageJson packageDir sdkVersion (Scope scope) depends =
  BSL.writeFile (packageDir </> "package.json") $
    encodePretty (packageJson (NpmPackageName name) (NpmPackageVersion version) dependencies)
  where
    version = versionToText sdkVersion
    name = packageNameOfPackageDir packageDir
    dependencies = HMS.fromList [ (NpmPackageName pkg, NpmPackageVersion version)
       | d <- depends
       , let pkg = scope <> "/" <> unDependency d
       ]
    packageNameOfPackageDir :: FilePath -> T.Text
    packageNameOfPackageDir packageDir = scope <> "/" <> package
      where
        package = T.pack $ takeFileName packageDir

    packageJson :: NpmPackageName -> NpmPackageVersion -> HMS.HashMap NpmPackageName NpmPackageVersion -> Value
    packageJson name version@(NpmPackageVersion sdkVersion) dependencies = object
      [ "private" .= True
      , "name" .= name
      , "version" .= version
      , "main" .= ("lib/index.js" :: T.Text)
      , "types" .= ("lib/index.d.ts" :: T.Text)
      , "description" .= ("Generated by daml2ts" :: T.Text)
      , "dependencies" .= (pkgDependencies config <> dependencies)
      , "devDependencies" .= pkgDevDependencies config
      , "scripts" .= pkgScripts config
      ]
      where
        config = configConsts sdkVersion

-- This type describes the format of a "top-level" 'package.json'. We
-- expect such files to have the format
-- {
--   "workspaces: [
--      "path/to/foo",
--      "path/to/bar",
--      ...
--   ],
--   ... perhaps other stuff ...
-- }

data PackageJson = PackageJson
  { workspaces :: [T.Text]
  , otherFields :: Object
  }
  deriving Show
instance Semigroup PackageJson where
  l <> r = PackageJson (workspaces l <> workspaces r) (otherFields l <> otherFields r)
instance Monoid PackageJson where
  mempty = PackageJson mempty mempty
instance FromJSON PackageJson where
  parseJSON (Object v) = PackageJson
      <$> v .: "workspaces"
      <*> pure (HMS.delete "workspaces" v)
  parseJSON _ = mzero
instance ToJSON PackageJson where
  toJSON PackageJson{..} = Object (HMS.insert "workspaces" (toJSON workspaces) otherFields)

-- Read the provided 'package.json'; transform it to include the
-- provided workspaces; write it back to disk.
setupWorkspace :: FilePath -> FilePath -> [(T.Text, [Dependency])] -> IO ()
setupWorkspace optInputPackageJson optOutputDir dependencies = do
  let (g, nodeFromVertex) = graphFromEdges'
        (map (\(a, ds) -> (a, a, map unDependency ds)) dependencies)
      ps = map (fst3 . nodeFromVertex) $ reverse (topSort g)
        -- Topologically order our packages.
      outBaseDir = T.pack $ takeFileName optOutputDir
        -- The leaf directory of the output directory (e.g. often 'daml2ts').
  let ourWorkspaces = map ((outBaseDir <> "/") <>) ps
  mbBytes <- catchJust (guard . isDoesNotExistError)
    (Just <$> BSL.readFile optInputPackageJson) (const $ pure Nothing)
  packageJson <- case mbBytes of
    Nothing -> pure mempty
    Just bytes -> case decode bytes :: Maybe PackageJson of
      Nothing -> fail $ "Error decoding JSON from '" <> optInputPackageJson <> "'"
      Just packageJson -> pure packageJson
  transformAndWrite ourWorkspaces outBaseDir packageJson
  where
    transformAndWrite :: [T.Text] -> T.Text -> PackageJson -> IO ()
    transformAndWrite ourWorkspaces outBaseDir packageJson = do
      let keepWorkspaces = filter (not . T.isPrefixOf outBaseDir) $ workspaces packageJson
            -- Old versions of our packages should be removed.
          allWorkspaces = ourWorkspaces ++ keepWorkspaces
            -- Our packages need to come before any other existing packages.
      BSL.writeFile optInputPackageJson $ encodePretty packageJson{workspaces=allWorkspaces}
      putStrLn $ "'" <> optInputPackageJson <> "' created or updated."
