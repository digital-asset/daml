-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

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
import qualified Data.Text.IO
import qualified "zip-archive" Codec.Archive.Zip as Zip
import qualified Data.Map as Map
import Data.Aeson hiding (Options)
import Data.Aeson.Encode.Pretty

import Control.Monad.Extra
import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Optics
import Data.Tuple.Extra
import Data.List.Extra
import Data.Graph
import Data.Maybe
import Data.Bifoldable
import Options.Applicative
import System.IO.Extra
import System.Directory
import System.FilePath hiding ((<.>))
import qualified System.FilePath as FP

import DA.Daml.Project.Consts

data Options = Options
    { optInputDars :: [FilePath]
    , optOutputDir :: FilePath
    , optDamlTypesVersion :: Maybe String
        -- The version string of '@daml/types' the packages we write should depend on.
        --    Default : SDK version if defined else 0.0.0.
    , optInputPackageJson :: Maybe FilePath
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
        <> help "Output directory for the generated TypeScript files"
        )
    <*> optional (strOption
        (  long "daml-types-version"
        <> help "The version of '@daml/types' to depend on"
        <> internal
        ))
    <*> optional (strOption
        (  short 'p'
        <> metavar "PACKAGE-JSON"
        <> help "Path to an existing 'package.json' to update"
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
    sdkVersion <- fromMaybe "0.0.0" <$> getSdkVersionMaybe
    let damlTypesVersion = fromMaybe sdkVersion optDamlTypesVersion
       -- The '@daml/types' version that the packages we write should depend on.
    putStrLn $ "Referencing '@daml/types' : \"" <> damlTypesVersion <> "\""
    let packageVersion   = sdkVersion
       -- The version string for packages we write.
    ps <- readPackages optInputDars
    case mergePackageMap ps of
      Left err -> fail . T.unpack $ err
      Right pm -> do
        dependencies <-
          forM (Map.toList pm) $
            \(pkgId, (mbPkgName, pkg)) -> do
                 let id = unPackageId pkgId
                     name = packageNameText pkgId mbPkgName
                     asName = if name == id then "itself" else name
                 Data.Text.IO.putStrLn $ "Generating " <> id <> " as " <> asName
                 deps <- daml2ts opts pm pkgId pkg mbPkgName damlTypesVersion packageVersion
                 pure (name, name, deps)
        whenJust optInputPackageJson $
          writeTopLevelPackageJson optOutputDir dependencies

packageNameText :: PackageId -> Maybe PackageName -> T.Text
packageNameText pkgId mbPkgIdent = maybe (unPackageId pkgId) unPackageName mbPkgIdent

newtype Scope = Scope {unscope :: String}
newtype Dependency = Dependency {undependency :: String}  deriving (Eq, Ord)

-- Gives the scope 'foo' given a directory path like '/path/to/foo'.
scopeOfScopeDir :: FilePath -> Scope
scopeOfScopeDir = Scope . takeFileName

-- Write the files for a single package.
daml2ts :: Options -> Map.Map PackageId (Maybe PackageName, Package) ->
    PackageId -> Package -> Maybe PackageName -> String -> String -> IO [Dependency]
daml2ts Options{..} pm pkgId pkg mbPkgName damlTypesVersion packageVersion = do
    let scopeDir = optOutputDir
          -- The directory into which we generate packages e.g. '/path/to/daml2ts'.
        packageDir = scopeDir </> T.unpack (packageNameText pkgId mbPkgName)
          -- The directory into which we write this package e.g. '/path/to/daml2ts/davl-0.0.4'.
        packageSrcDir = packageDir </> "src"
          -- Where the source files of this package are written e.g. '/path/to/daml2ts/davl-0.0.4/src'.
        scope = scopeOfScopeDir scopeDir
          -- The scope e.g. 'daml2ts'.
          -- We use this, for example, when generating import declarations e.g.
          --   import * as pkgd14e08_DA_Internal_Template from '@daml2ts/d14e08/lib/DA/Internal/Template';
    createDirectoryIfMissing True packageSrcDir
    -- Write .ts files for the package and harvest references to
    -- foreign packages as we do.
    T.writeFileUtf8 (packageSrcDir </> "packageId.ts") $ T.unlines
        ["export default '" <> unPackageId pkgId <> "';"]
    dependencies <- nubOrd . concat <$> mapM (writeModuleTs packageSrcDir scope) (NM.toList (packageModules pkg))
    -- Now write 'package.json', 'tsconfig.json' and '.eslint.rc.json'
    -- files into the package dir. The 'package.json' needs the
    -- dependencies.
    writeTsConfig packageDir
    writeEsLintConfig packageDir
    writePackageJson packageDir damlTypesVersion packageVersion scope dependencies
    pure dependencies
    where
      -- Write the .ts file for a single DAML-LF module.
      writeModuleTs :: FilePath -> Scope -> Module -> IO [Dependency]
      writeModuleTs packageSrcDir scope mod = do
         maybe (pure [])
           (\(modTxt, ds) -> do
             let outputFile = packageSrcDir </> joinPath (map T.unpack (unModuleName (moduleName mod))) FP.<.> "ts"
             createDirectoryIfMissing True (takeDirectory outputFile)
             T.writeFileUtf8 outputFile modTxt
             pure ds
           )
           (genModule pm scope pkgId mod)

-- Generate the .ts content for a single module.
genModule :: Map.Map PackageId (Maybe PackageName, Package) ->
     Scope -> PackageId -> Module -> Maybe (T.Text, [Dependency])
genModule pm (Scope scope) curPkgId mod
  | null serDefs =
    Nothing -- If no serializable types, nothing to do.
  | otherwise =
    let (defSers, refs) = unzip (map (genDataDef curPkgId mod tpls) serDefs)
        imports = [ importDecl pm modRef rootPath
                  | modRef@(pkgRef, _) <- modRefs refs
                  , let rootPath = pkgRootPath modName pkgRef ]
        defs = map biconcat defSers
        modText = T.unlines $ intercalate [""] $ filter (not . null) $ modHeader : imports : defs
        depends = [ Dependency . T.unpack $ pkgRefStr pm pkgRef
                  | (pkgRef, _) <- modRefs refs, pkgRef /= PRSelf ]
   in Just (modText, depends)
  where
    modName = moduleName mod
    tpls = moduleTemplates mod
    serDefs = defDataTypes mod
    modRefs refs = Set.toList ((PRSelf, modName) `Set.delete` Set.unions refs)
    modHeader =
      [ "// Generated from " <> T.intercalate "/" (unModuleName modName) <> ".daml"
      , "/* eslint-disable @typescript-eslint/camelcase */"
      , "/* eslint-disable @typescript-eslint/no-use-before-define */"
      , "import * as jtv from '@mojotech/json-type-validation';"
      , "import * as daml from '@daml/types';"
      ]

    -- Calculate an import declaration.
    importDecl :: Map.Map PackageId (Maybe PackageName, Package) ->
                      (PackageRef, ModuleName) -> T.Text -> T.Text
    importDecl pm modRef@(pkgRef, modName) rootPath =
      "import * as " <>  genModuleRef modRef <> " from '" <>
      T.intercalate "/" ((rootPath : pkgRefStr pm pkgRef : ["lib" | pkgRef /= PRSelf]) ++ unModuleName modName) <>
      "';"

    -- Produce a package name for a package ref.
    pkgRefStr :: Map.Map PackageId (Maybe PackageName, Package) -> PackageRef -> T.Text
    pkgRefStr pm = \case
      PRSelf -> ""
      PRImport pkgId ->
        maybe (error "IMPOSSIBLE : package map malformed")
        (\(mbPkgName, _) -> packageNameText pkgId mbPkgName)
        (Map.lookup pkgId pm)

    -- Calculate the base part of a package ref string. For foreign
    -- imports that's something like '@daml2ts'. For self refences
    -- something like '../../'.
    pkgRootPath :: ModuleName -> PackageRef -> T.Text
    pkgRootPath modName pkgRef =
      case pkgRef of
        PRSelf ->
          if lenModName == 1
            then "."
            else T.intercalate "/" (replicate (lenModName - 1) "..")
        PRImport _ -> T.pack $ "@" <> scope
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
                     -- To-do: Can we formulate a static implements
                     -- serializable check that works for companion
                     -- functions?
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
                  in ["export const " <> conName <> ":\n  " <> typ' <> " = ({"] ++ body ++ ["});"] ++
                     ["daml.STATIC_IMPLEMENTS_SERIALIZABLE_CHECK<" <> conName <> ">(" <> conName <> ")"]
            in ((typeDesc, serDesc), Set.unions $ map (Set.setOf typeModuleRef . snd) bs)
        DataEnum enumCons ->
          let
            typeDesc =
                [ "export enum " <> conName <> " {"] ++
                [ "  " <> cons <> " = " <> "\'" <> cons <> "\'" <> ","
                | VariantConName cons <- enumCons] ++
                [ "}"
                , "daml.STATIC_IMPLEMENTS_SERIALIZABLE_CHECK<" <> conName <> ">(" <> conName <> ")"
                ]
            serDesc =
                ["() => jtv.oneOf<" <> conName <> ">" <> "("] ++
                ["  jtv.constant(" <> conName <> "." <> cons <> ")," | VariantConName cons <- enumCons] ++
                [");"]
          in
          ((typeDesc, makeNameSpace serDesc), Set.empty)
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
        makeNameSpace serDesc =
            [ "// eslint-disable-next-line @typescript-eslint/no-namespace"
            , "export namespace " <> conName <> " {"
            ] ++
            map ("  " <>) (onHead ("export const decoder = " <>) serDesc) ++
            ["}"]
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
    PRSelf -> modNameStr
    PRImport pkgId -> "pkg" <> unPackageId pkgId <> "_" <> modNameStr
  where
    modNameStr = T.intercalate "_" (unModuleName modName)

onHead :: (a -> a) -> [a] -> [a]
onHead f = \case
    [] -> []
    x : xs -> f x : xs

onLast :: (a -> a) -> [a] -> [a]
onLast f = \case
    [] -> []
    [l] -> [f l]
    x : xs -> x : onLast f xs

writeTsConfig :: FilePath -> IO ()
writeTsConfig dir = writeFileUTF8 (dir </> "tsconfig.json") $ unlines
    [ "{"
    , "  \"compilerOptions\": {"
    , "    \"target\": \"es5\","
    , "    \"lib\": ["
    , "      \"dom\","
    , "      \"es2015\""
    , "     ],"
    , "    \"strict\": true,"
    , "    \"noUnusedLocals\": true,"
    , "    \"noUnusedParameters\": false,"
    , "    \"noImplicitReturns\": true,"
    , "    \"noFallthroughCasesInSwitch\": true,"
    , "    \"outDir\": \"lib\","
    , "    \"module\": \"commonjs\","
    , "    \"declaration\": true,"
    , "    \"sourceMap\": true"
    , "    },"
    , "  \"include\": [\"src/**/*.ts\"],"
    , "}"
    ]

writeEsLintConfig :: FilePath -> IO ()
writeEsLintConfig dir = writeFileUTF8 (dir </> ".eslintrc.json") $ unlines
  [ "{"
  , "  \"parser\": \"@typescript-eslint/parser\","
  , "  \"parserOptions\": {"
  , "    \"project\": \"./tsconfig.json\""
  , "  },"
  , "  \"plugins\": ["
  , "    \"@typescript-eslint\""
  , "  ],"
  , "  \"extends\": ["
  , "    \"eslint:recommended\","
  , "    \"plugin:@typescript-eslint/eslint-recommended\","
  , "    \"plugin:@typescript-eslint/recommended\","
  , "    \"plugin:@typescript-eslint/recommended-requiring-type-checking\""
  , "  ],"
  , "  \"rules\": {"
  , "    \"@typescript-eslint/explicit-function-return-type\": \"off\","
  , "    \"@typescript-eslint/no-inferrable-types\": \"off\""
  , "  }"
  , "}"
  ]

writePackageJson :: FilePath -> String -> String -> Scope -> [Dependency] -> IO ()
writePackageJson packageDir damlTypesVersion packageVersion (Scope scope) depends =
  writeFileUTF8 (packageDir </> "package.json") $ unlines
  (["{"
   , "  \"private\": true,"
   , "  \"name\": \"" <> packageName <> "\","
   , "  \"version\": \"" <> packageVersion <> "\","
   , "  \"description\": \"Produced by daml2ts\","
   , "  \"license\": \"Apache-2.0\","
   , "  \"dependencies\": {"
   , "    \"@daml/types\": \"" <> damlTypesVersion <> "\","
   , "    \"@mojotech/json-type-validation\": \"^3.1.0\"" ++ if not $ null dependencies then ", " else ""
   ] ++ dependencies ++
    ["  },"
    , "  \"scripts\": {"
    , "    \"build\": \"tsc --build\","
    , "    \"lint\": \"eslint --ext .ts src/ --max-warnings 0\""
    , "  },"
    , "  \"devDependencies\": {"
    , "    \"@typescript-eslint/eslint-plugin\": \"^2.11.0\","
    , "    \"@typescript-eslint/parser\": \"^2.11.0\","
    , "    \"eslint\": \"^6.7.2\","
    , "    \"typescript\": \"~3.7.3\""
    , "  }"
    , "}"
    ])
  where
    packageName =  packageNameOfPackageDir packageDir
    dependencies = withCommas [ "    \"" <> pkg <> "\": \"" <> packageVersion <> "\""
                              | d <- depends
                              , let pkg = "@" ++ scope ++ "/" ++ undependency d
                              ]

    -- From the path to a package like '/path/to/daml2ts/d14e08'
    -- calculates '@daml2ts/d14e08' suitable for use as the "name" field
    -- of a 'package.json'.
    packageNameOfPackageDir :: FilePath -> String
    packageNameOfPackageDir packageDir = "@" <> scope <> "/" <> package
      where
        scope = unscope $ scopeOfScopeDir (takeDirectory packageDir)
        package = takeFileName packageDir

    withCommas :: [String] -> [String]
    withCommas [] = []
    withCommas ms = reverse (head ms' : map (++ ",") (tail ms')) where ms' = reverse ms

-- This type describes the format of a "top-level" 'package.json'. We
-- expect such files to have the format
-- {
--   "workspaces: [
--      "path/to/foo",
--      "path/to/bar",
--      ...
--   ],
--   ...
-- }
data PackageJson = PackageJson
  { workspaces :: [T.Text]
  , otherFields :: Object
  } deriving Show
-- Explicitly provide instances to avoid relying on restricted
-- extension 'DeriveGeneric'.
instance FromJSON PackageJson where
  parseJSON (Object v) = PackageJson
      <$> v .: "workspaces"
      <*> pure (HMS.delete "workspaces" v)
  parseJSON _ = mzero
instance ToJSON PackageJson where
  toJSON PackageJson{..} = Object (HMS.insert "workspaces" (toJSON workspaces) otherFields)

-- Read the provided 'package.json'; transform it to include the
-- provided workspaces; write it back to disk.
writeTopLevelPackageJson :: FilePath -> [(T.Text, T.Text, [Dependency])] -> FilePath -> IO ()
writeTopLevelPackageJson optOutputDir dependencies file = do
  let (g, nodeFromVertex) = graphFromEdges'
        (map (\(a, b, ds) -> (T.unpack a, T.unpack b, map undependency ds)) dependencies)
      ps = map (fst3 . nodeFromVertex) $ reverse (topSort g)
        -- Topologically order our packages.
      ldr = unscope $ scopeOfScopeDir optOutputDir
        -- 'ldr' we expect to be something like "daml2ts".
  let ourPackages = map (T.pack . ((ldr ++ "/") ++)) ps
  bytes <- BSL.readFile file
  maybe
    (fail $ "Error decoding JSON from '" <> file <> "'")
    (transformAndWrite ourPackages ldr)
    (decode bytes :: Maybe PackageJson)
  where
    transformAndWrite :: [T.Text] -> String -> PackageJson -> IO ()
    transformAndWrite ourPackages ldr oldPackageJson = do
      let damlTypes = [T.pack "daml-types"]
      --   * Old versions of our packages should be removed;
          keepPackages =
            [ T.pack x
            | x <- [T.unpack y | y <- workspaces oldPackageJson]
            , isNothing $ stripPrefix ldr x
            ]
      --  * Our packages need to come after 'daml-types' if it exists;
      --  * Our packages need to come before any other existing packages.
          allPackages = maybe
            (ourPackages ++ keepPackages)
            (\(before, after) -> before ++ damlTypes ++ ourPackages ++ after)
            (stripInfix damlTypes keepPackages)
      BSL.writeFile file $ encodePretty oldPackageJson{workspaces=allPackages}
      putStrLn $ "'" <> file <> "' updated."
