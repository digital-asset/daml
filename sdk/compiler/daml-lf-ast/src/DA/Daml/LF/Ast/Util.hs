-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TemplateHaskell #-}
module DA.Daml.LF.Ast.Util(module DA.Daml.LF.Ast.Util) where

import Control.DeepSeq
import           Control.Lens
import           Control.Lens.Ast
import Control.Monad
import Data.List
import Data.Maybe
import qualified Data.Text as T
import           Data.Data
import           Data.Functor.Foldable
import qualified Data.Graph as G
import Data.List.Extra (nubSort, stripInfixEnd)
import qualified Data.NameMap as NM
import           GHC.Generics (Generic)

import Module (UnitId, unitIdString, stringToUnitId)
import System.FilePath
import Text.Read (readMaybe)

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.TypeLevelNat
import DA.Daml.LF.Ast.Optics
import DA.Daml.LF.Ast.Recursive
import DA.Daml.LF.Ast.Version

dvalName :: DefValue -> ExprValName
dvalName = fst . dvalBinder

dvalType :: DefValue -> Type
dvalType = snd . dvalBinder

chcArgType :: TemplateChoice -> Type
chcArgType = snd . chcArgBinder

-- Return topologically sorted packages, with the parent packages before their
-- dependencies
sortPackagesParentFirst :: [(PackageId, a, Package)] -> Either [(PackageId, a, Package)] [(PackageId, a, Package)]
sortPackagesParentFirst pkgs =
  let toPkgNode x@(pkgId, _, pkg) =
        ( x
        , pkgId
        , toListOf (packageRefs . _ImportedPackageId) pkg
        )
      fromPkgNode (x, _pkgId, _deps) = x
      sccs = G.stronglyConnCompR (map toPkgNode pkgs)
      isAcyclic = \case
        G.AcyclicSCC pkg -> Right pkg
        -- A package referencing itself shouldn't happen, but is not an actually
        -- problematic cycle and won't trip up the engine
        G.CyclicSCC [pkg] -> Right pkg
        G.CyclicSCC pkgCycle -> Left (map fromPkgNode pkgCycle)
  in
  reverse . map fromPkgNode <$> traverse isAcyclic sccs

topoSortPackage :: Package -> Either [ModuleName] Package
topoSortPackage pkg@Package{packageModules = mods} = do
  let isLocal (pkgRef, modName) = case pkgRef of
        SelfPackageId -> Just modName
        ImportedPackageId{} -> Nothing
  let modDeps = nubSort . mapMaybe isLocal . toListOf moduleModuleRef
  let modNode mod0 = (mod0, moduleName mod0, modDeps mod0)
  let sccs = G.stronglyConnComp (map modNode (NM.toList mods))
  let isAcyclic = \case
        G.AcyclicSCC mod0 -> Right mod0
        -- NOTE(MH): A module referencing itself is not really a cycle.
        G.CyclicSCC [mod0] -> Right mod0
        G.CyclicSCC modCycle -> Left (map moduleName modCycle)
  mods <- traverse isAcyclic sccs
  pure pkg { packageModules = NM.fromList mods }

isUtilityPackage :: Package -> Bool
isUtilityPackage pkg =
  all (\mod ->
    null (moduleTemplates mod)
      && null (moduleInterfaces mod)
      && not (any (getIsSerializable . dataSerializable) $ moduleDataTypes mod)
  ) $ packageModules pkg

mergeImportedPackages' :: ImportedPackages -> ImportedPackages -> ImportedPackages
mergeImportedPackages' r l = case (r, l) of
    (Right s1, Right s2) -> Right $ s1 <> s2
    (Left  r1, Left  r2) -> Left $ combineReasons r1 r2
    (Right s1, Left  _ ) -> Right s1
    (Left  _ , Right s2) -> Right s2
    where
      combineReasons StablePackage StablePackage = StablePackage
      combineReasons (Combined xs) (Combined ys) = Combined (xs ++ ys)
      combineReasons (Combined xs) y             = Combined (xs ++ [y])
      combineReasons x             (Combined ys) = Combined (x:ys)
      combineReasons x             y             = Combined [x, y]

data Arg
  = TmArg Expr
  | TyArg Type

mkEApp :: Expr -> Arg -> Expr
mkEApp e (TmArg a) = ETmApp e a
mkEApp e (TyArg t) = ETyApp e t

_EApp :: Prism' Expr (Expr, Arg)
_EApp = prism' inj proj
  where
    inj (f, a) = case a of
      TmArg e -> ETmApp f e
      TyArg t -> ETyApp f t
    proj = \case
      ETmApp f e -> Just (f, TmArg e)
      ETyApp f t -> Just (f, TyArg t)
      _          -> Nothing

_ETmApps :: Iso' Expr (Expr, [Expr])
_ETmApps = leftSpine _ETmApp

_ETyApps :: Iso' Expr (Expr, [Type])
_ETyApps = leftSpine _ETyApp

_EApps :: Iso' Expr (Expr, [Arg])
_EApps = leftSpine _EApp

_ETmLams :: Iso' Expr ([(ExprVarName, Type)], Expr)
_ETmLams = rightSpine _ETmLam

_ETyLams :: Iso' Expr ([(TypeVarName, Kind)], Expr)
_ETyLams = rightSpine _ETyLam

_ELets :: Iso' Expr ([Binding], Expr)
_ELets = rightSpine _ELet

mkETmApps :: Expr -> [Expr] -> Expr
mkETmApps = curry (review _ETmApps)

mkETyApps :: Expr -> [Type] -> Expr
mkETyApps = curry (review _ETyApps)

mkEApps :: Expr -> [Arg] -> Expr
mkEApps = curry (review _EApps)

mkETmLams :: [(ExprVarName, Type)] -> Expr -> Expr
mkETmLams = curry (review _ETmLams)

mkETyLams :: [(TypeVarName, Kind)] -> Expr -> Expr
mkETyLams = curry (review _ETyLams)

mkELets :: [Binding] -> Expr -> Expr
mkELets = curry (review _ELets)

mkEmptyText :: Expr
mkEmptyText = EBuiltinFun (BEText "")

mkIf :: Expr -> Expr -> Expr -> Expr
mkIf cond0 then0 else0 =
  ECase cond0
  [ CaseAlternative (CPBool True ) then0
  , CaseAlternative (CPBool False) else0
  ]

mkBool :: Bool -> Expr
mkBool = EBuiltinFun . BEBool

pattern EUnit :: Expr
pattern EUnit = EBuiltinFun BEUnit

pattern ETrue :: Expr
pattern ETrue = EBuiltinFun (BEBool True)

pattern EFalse :: Expr
pattern EFalse = EBuiltinFun (BEBool False)

mkNot :: Expr -> Expr
mkNot arg = mkIf arg (mkBool False) (mkBool True)

mkOr :: Expr -> Expr -> Expr
mkOr arg1 arg2 = mkIf arg1 (mkBool True) arg2

mkAnd :: Expr -> Expr -> Expr
mkAnd arg1 arg2 = mkIf arg1 arg2 (mkBool False)

mkAnds :: [Expr] -> Expr
mkAnds [] = mkBool True
mkAnds [x] = x
mkAnds (x:xs) = mkAnd x $ mkAnds xs


alpha, beta, gamma :: TypeVarName
-- NOTE(MH): We want to avoid shadowing variables in the environment. That's
-- what the weird names are for.
alpha = TypeVarName "::alpha::"
beta  = TypeVarName "::beta::"
gamma = TypeVarName "::gamma::"

tAlpha, tBeta, tGamma :: Type
tAlpha = TVar alpha
tBeta  = TVar beta
tGamma = TVar gamma


infixr 1 :->

-- | Type constructor for function types.
pattern (:->) :: Type -> Type -> Type
pattern a :-> b = TArrow `TApp` a `TApp` b

pattern TUnit, TBool, TInt64, TText, TTimestamp, TParty, TDate, TArrow, TNumeric10, TAny, TNat10, TTypeRep, TAnyException, TRoundingMode, TBigNumeric, TFailureCategory :: Type
pattern TUnit       = TBuiltin BTUnit
pattern TBool       = TBuiltin BTBool
pattern TInt64      = TBuiltin BTInt64
pattern TNumeric10  = TNumeric TNat10 -- new decimal
pattern TNat10      = TNat TypeLevelNat10
pattern TText       = TBuiltin BTText
pattern TTimestamp  = TBuiltin BTTimestamp
pattern TParty      = TBuiltin BTParty
pattern TDate       = TBuiltin BTDate
pattern TArrow      = TBuiltin BTArrow
pattern TAny        = TBuiltin BTAny
pattern TTypeRep    = TBuiltin BTTypeRep
pattern TRoundingMode = TBuiltin BTRoundingMode
pattern TBigNumeric  = TBuiltin BTBigNumeric
pattern TAnyException = TBuiltin BTAnyException
pattern TFailureCategory = TBuiltin BTFailureCategory

pattern TList, TOptional, TTextMap, TUpdate, TContractId, TNumeric :: Type -> Type
pattern TList typ = TApp (TBuiltin BTList) typ
pattern TOptional typ = TApp (TBuiltin BTOptional) typ
pattern TTextMap typ = TApp (TBuiltin BTTextMap) typ
pattern TUpdate typ = TApp (TBuiltin BTUpdate) typ
pattern TContractId typ = TApp (TBuiltin BTContractId) typ
pattern TNumeric n = TApp (TBuiltin BTNumeric) n

pattern TGenMap :: Type -> Type -> Type
pattern TGenMap t1 t2 = TApp (TApp (TBuiltin BTGenMap) t1) t2

pattern TTextMapEntry :: Type -> Type
pattern TTextMapEntry a = TStruct [(FieldName "key", TText), (FieldName "value", a)]

pattern TTuple2 :: Type -> Type -> Type
pattern TTuple2 t1 t2 = TApp (TApp (TCon Tuple2TCon)  t1) t2

pattern Tuple2TCon :: Qualified TypeConName
pattern Tuple2TCon = (Qualified
  -- We cannot look up these stable IDs using stablePackageByModuleName because
  -- it would introduce a cyclic dependency with StablePackages.
    (ImportedPackageId (PackageId "5aee9b21b8e9a4c4975b5f4c4198e6e6e8469df49e2010820e792f393db870f4"))
    (ModuleName ["DA", "Types"])
    (TypeConName ["Tuple2"])
  )

pattern TConApp :: Qualified TypeConName -> [Type] -> Type
pattern TConApp tcon targs <- (view (leftSpine _TApp) -> (TCon tcon, targs))
  where
    TConApp tcon targs = foldl' TApp (TCon tcon) targs

pattern TForalls :: [(TypeVarName, Kind)] -> Type -> Type
pattern TForalls binders ty <- (view _TForalls -> (binders, ty))
  where TForalls binders ty = mkTForalls binders ty

_TList :: Prism' Type Type
_TList = prism' TList $ \case
  TList typ -> Just typ
  _ -> Nothing

_TOptional :: Prism' Type Type
_TOptional = prism' TOptional $ \case
  TOptional typ -> Just typ
  _ -> Nothing

_TUpdate :: Prism' Type Type
_TUpdate = prism' TUpdate $ \case
  TUpdate typ -> Just typ
  _ -> Nothing

_TNumeric :: Prism' Type Type
_TNumeric = prism' TNumeric $ \case
  TNumeric n -> Just n
  _ -> Nothing

_TConApp :: Prism' Type (Qualified TypeConName, [Type])
_TConApp = prism' (uncurry TConApp) $ \case
  TConApp tcon targs -> Just (tcon, targs)
  _ -> Nothing

_TForalls :: Iso' Type ([(TypeVarName, Kind)], Type)
_TForalls = rightSpine _TForall

_TApps :: Iso' Type (Type, [Type])
_TApps = leftSpine _TApp

mkTForalls :: [(TypeVarName, Kind)] -> Type -> Type
mkTForalls binders ty = foldr TForall ty binders

mkTFuns :: [Type] -> Type -> Type
mkTFuns ts t = foldr (:->) t ts

mkTApps :: Type -> [Type] -> Type
mkTApps = curry (review _TApps)

splitTApps :: Type -> (Type, [Type])
splitTApps = view _TApps


typeConAppToType :: TypeConApp -> Type
typeConAppToType (TypeConApp tcon targs) = TConApp tcon targs


-- Compatibility type and functions

data Definition
  = DTypeSyn DefTypeSyn
  | DDataType DefDataType
  | DValue DefValue
  | DTemplate Template
  | DException DefException
  | DInterface DefInterface
  deriving Show

moduleFromDefinitions :: ModuleName -> Maybe FilePath -> FeatureFlags -> [Definition] -> Module
moduleFromDefinitions name path flags defs = do
  let (syns, dats, vals, tpls, exps, ifs) = partitionDefinitions defs
  Module name path flags (NM.fromList syns) (NM.fromList dats) (NM.fromList vals) (NM.fromList tpls) (NM.fromList exps) (NM.fromList ifs)

partitionDefinitions :: [Definition] -> ([DefTypeSyn], [DefDataType], [DefValue], [Template], [DefException], [DefInterface])
partitionDefinitions = foldr f ([], [], [], [], [], [])
  where
    f = \case
      DTypeSyn s  -> over _1 (s:)
      DDataType d -> over _2 (d:)
      DValue v    -> over _3 (v:)
      DTemplate t -> over _4 (t:)
      DException e -> over _5 (e:)
      DInterface i -> over _6 (i:)

-- | All names of top level exportable definitions (does not include data constructors/record accessors, as they are covered by the type name)
topLevelExportables :: [Definition] -> [T.Text]
topLevelExportables defs =
  let (syns, dataTypes, values, templates, exceptions, interfaces) = partitionDefinitions defs
   in mconcat
        [ last . unTypeSynName . synName <$> syns
        , last . unTypeConName . dataTypeCon <$> dataTypes
        , unExprValName . fst . dvalBinder <$> values
        , last . unTypeConName . tplTypeCon <$> templates -- Template names
        , mconcat $ fmap (unChoiceName . chcName) . NM.elems . tplChoices <$> templates -- Template Choice names
        , last . unTypeConName . exnName <$> exceptions
        , last . unTypeConName . intName <$> interfaces -- Interface names
        , mconcat $ fmap (unChoiceName . chcName) . NM.elems . intChoices <$> interfaces -- Interface Choice names
        ]

-- | This is the analogue of GHC’s moduleNameString for the LF
-- `ModuleName` type.
moduleNameString :: ModuleName -> T.Text
moduleNameString = T.intercalate "." . unModuleName

packageModuleNames :: Package -> [T.Text]
packageModuleNames = map (moduleNameString . moduleName) . NM.elems . packageModules

-- | Remove all location information from an expression.
removeLocations :: Expr -> Expr
removeLocations = cata $ \case
    ELocationF _loc e -> e
    b -> embed b

-- | Given the name of a DALF and the decoded package return package metadata.
--
-- Extract the package metadata from the provided package, but returns no
-- package version for daml-print, which GHC insists on not having a version.
safePackageMetadata :: Package -> (PackageName, Maybe PackageVersion)
safePackageMetadata (packageMetadata -> PackageMetadata name version _) =
    (name, version <$ guard (name /= PackageName "daml-prim"))

-- Get the name of a file and an expeted package id of the package, get the unit id
-- by stripping away the package name at the end.
-- E.g., if 'package-name-123abc' is given and the known package id is
-- '123abc', then 'package-name' is returned as unit id.
unitIdFromFile :: FilePath -> PackageId -> UnitId
unitIdFromFile file (PackageId pkgId) =
    (stringToUnitId . fromMaybe name . stripPkgId name . T.unpack) pkgId
    where name = takeBaseName file

-- Strip the package id from the end of a dalf file name
-- TODO (drsk) This needs to become a hard error
stripPkgId :: String -> String -> Maybe String
stripPkgId baseName expectedPkgId = do
    (unitId, pkgId) <- stripInfixEnd "-" baseName
    guard $ pkgId == expectedPkgId
    pure unitId

-- | Take a string of the form "daml-stdlib-0.13.43" and split it into ("daml-stdlib", Just "0.13.43")
splitUnitId :: UnitId -> (PackageName, Maybe PackageVersion)
splitUnitId (unitIdString -> unitId) = fromMaybe (PackageName (T.pack unitId), Nothing) $ do
    (name, ver) <- stripInfixEnd "-" unitId
    guard $ all (`elem` '.' : ['0' .. '9']) ver
    pure (PackageName (T.pack name), Just (PackageVersion (T.pack ver)))

-- | Take a package version of regex "(0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*" into
-- a list of integers [Integer]
splitPackageVersion
  :: (PackageVersion -> a) -> PackageVersion
  -> Either a RawPackageVersion
splitPackageVersion mkError version@(PackageVersion raw) =
  let pieces = T.split (== '.') raw
  in
  case traverse (readMaybe . T.unpack) pieces of
    Nothing -> Left (mkError version)
    Just versions -> Right $ RawPackageVersion versions

newtype RawPackageVersion = RawPackageVersion [Integer]

padEquivalent :: RawPackageVersion -> RawPackageVersion -> ([Integer], [Integer])
padEquivalent (RawPackageVersion v1Pieces) (RawPackageVersion v2Pieces) =
  let pad xs target =
        take
          (length target `max` length xs)
          (xs ++ repeat 0)
  in
  (pad v1Pieces v2Pieces, pad v2Pieces v1Pieces)

instance Ord RawPackageVersion where
  compare v1 v2 = uncurry compare $ padEquivalent v1 v2

instance Eq RawPackageVersion where
  (==) v1 v2 = uncurry (==) $ padEquivalent v1 v2

instance Show RawPackageVersion where
  show (RawPackageVersion pieces) = intercalate "." $ map show pieces

data Upgrading a = Upgrading
    { _past :: a
    , _present :: a
    }
    deriving (Eq, Data, Generic, NFData, Show)

makeLenses ''Upgrading

instance Functor Upgrading where
    fmap f Upgrading{..} = Upgrading (f _past) (f _present)

instance Foldable Upgrading where
    foldMap f Upgrading{..} = f _past <> f _present

instance Traversable Upgrading where
    traverse f Upgrading{..} = Upgrading <$> f _past <*> f _present

instance Applicative Upgrading where
    pure a = Upgrading a a
    (<*>) f a = Upgrading { _past = _past f (_past a), _present = _present f (_present a) }

foldU :: (a -> a -> b) -> Upgrading a -> b
foldU f u = f (_past u) (_present u)

unsafeZipUpgrading :: Upgrading [a] -> [Upgrading a]
unsafeZipUpgrading = foldU (zipWith Upgrading)

unfoldU :: (Upgrading a -> b) -> a -> a -> b
unfoldU f past present = f Upgrading { _past = past, _present = present }

data UpgradingDep = UpgradingDep
  { udPkgName :: PackageName
  , udMbPackageVersion :: Maybe RawPackageVersion
  , udVersionSupportsUpgrades :: Bool
  , udIsUtilityPackage :: Bool
  , udPkgId :: PackageId
  }
  deriving (Eq)

instance Show UpgradingDep where
  show UpgradingDep {..} = T.unpack (unPackageName udPkgName) <> " (" <> T.unpack (unPackageId udPkgId) <> ")" <>
    case udMbPackageVersion of
      Just udPackageVersion -> " (v" <> show udPackageVersion <> ")"
      Nothing -> mempty

------------------------------------------------------------------------
-- Shorthands
------------------------------------------------------------------------
-- Exprs
eVar :: T.Text -> Expr
eVar = EVar . ExprVarName

eQualTest :: a -> Qualified a
eQualTest x = Qualified SelfPackageId (ModuleName ["Main"]) x

eValTest :: T.Text -> Expr
eValTest = EVal . eQualTest . ExprValName

-- Types
tvar :: T.Text -> Type
tvar = TVar . TypeVarName

tyLamTyp :: Type
tyLamTyp = TForall (a, typToTyp) (tvar "a" :-> tvar "a")
  where
    a = TypeVarName "a"
    typToTyp = KArrow KStar KStar

tconTest :: T.Text -> Type
tconTest t = TCon $ Qualified SelfPackageId (ModuleName ["Main"]) (TypeConName [t])

tsynTest :: T.Text -> [Type] -> Type
tsynTest t = TSynApp $ Qualified SelfPackageId (ModuleName ["Main"]) (TypeSynName [t])

tmyFuncTest :: Type -> Type
tmyFuncTest = TApp (tconTest "MyFunc")

-- Modules
mkEmptyModule :: Module
mkEmptyModule = Module{..}
  where
    moduleName :: ModuleName
    moduleName = ModuleName ["test"]
    moduleSource :: (Maybe FilePath)
    moduleSource = Nothing
    moduleFeatureFlags :: FeatureFlags
    moduleFeatureFlags = FeatureFlags
    moduleSynonyms :: (NM.NameMap DefTypeSyn)
    moduleSynonyms = NM.empty
    moduleDataTypes :: (NM.NameMap DefDataType)
    moduleDataTypes = NM.empty
    moduleValues :: (NM.NameMap DefValue)
    moduleValues = NM.empty
    moduleTemplates :: (NM.NameMap Template)
    moduleTemplates = NM.empty
    moduleExceptions :: (NM.NameMap DefException)
    moduleExceptions = NM.empty
    moduleInterfaces :: (NM.NameMap DefInterface)
    moduleInterfaces = NM.empty

-- Packages

-- NOTE: this is to be used for TESTING only, NOT for merging with other
-- packages.
mkOneModulePackageForTest :: Module -> Package
mkOneModulePackageForTest m = Package{..}
  where
    packageLfVersion = Version V2 PointDev
    packageModules = NM.fromList [m]
    importedPackages = Left $ Trace "DA.Daml.LF.Ast.Util:mkOneModulePackage" --since used for testing
    packageMetadata = PackageMetadata{..}
      where
        packageName :: PackageName
        packageName = PackageName "test"
        packageVersion :: PackageVersion
        packageVersion = PackageVersion "0.0"
        upgradedPackageId :: Maybe UpgradedPackageId
        upgradedPackageId = Nothing

{-
We put a list of stablepackages here, to be used by the proto encoder. These
cannot get the genereated list by DA.Daml.StablePackages because that would
introduce a circular dependency (DA.Daml.StablePackages depends on the encoder).
If changes occur to the stablepackages, we can bootstrap this list: compile with
current list of stable packages, observe hash, add has to list below. Since new
stable packages will only depend on existing stable packages, adding its hash to
this list won't change the hash.
-}
stableIds :: [PackageId]
stableIds = map PackageId
      [ "54f85ebfc7dfae18f7d70370015dcc6c6792f60135ab369c44ae52c6fc17c274" -- daml-prim
      , "ee33fb70918e7aaa3d3fc44d64a399fb2bf5bcefc54201b1690ecd448551ba88" -- daml-prim-DA-Exception-ArithmeticError
      , "6da1f43a10a179524e840e7288b47bda213339b0552d92e87ae811e52f59fc0e" -- daml-prim-DA-Exception-AssertionFailed
      , "f181cd661f7af3a60bdaae4b0285a2a67beb55d6910fc8431dbae21a5825ec0f" -- daml-prim-DA-Exception-GeneralError
      , "91e167fa7a256f21f990c526a0a0df840e99aeef0e67dc1f5415b0309486de74" -- daml-prim-DA-Exception-PreconditionFailed
      , "0e4a572ab1fb94744abb02243a6bbed6c78fc6e3c8d3f60c655f057692a62816" -- daml-prim-DA-Internal-Erased
      , "e5411f3d75f072b944bd88e652112a14a3d409c491fd9a51f5f6eede6d3a3348" -- daml-prim-DA-Internal-NatSyn-
      , "ab068e2f920d0e06347975c2a342b71f8b8e3b4be0f02ead9442caac51aa8877" -- daml-prim-DA-Internal-PromotedText
      , "5aee9b21b8e9a4c4975b5f4c4198e6e6e8469df49e2010820e792f393db870f4" -- daml-prim-DA-Types
      , "fcee8dfc1b81c449b421410edd5041c16ab59c45bbea85bcb094d1b17c3e9df7" -- daml-prim-GHC-Prim
      , "19f0df5fdaf5a96e137b6ea885fdb378f37bd3166bd9a47ee11518e33fa09a20" -- daml-prim-GHC-Tuple
      , "e7e0adfa881e7dbbb07da065ae54444da7c4bccebcb8872ab0cb5dcf9f3761ce" -- daml-prim-GHC-Types
      , "a1fa18133ae48cbb616c4c148e78e661666778c3087d099067c7fe1868cbb3a1" -- daml-stdlib-DA-Action-State-Type
      , "fa79192fe1cce03d7d8db36471dde4cf6c96e6d0f07e1c391dd49e355af9b38c" -- daml-stdlib-DA-Date-Types
      , "6f8e6085f5769861ae7a40dccd618d6f747297d59b37cab89b93e2fa80b0c024" -- daml-stdlib-DA-Internal-Any
      , "86d888f34152dae8729900966b44abcb466b9c111699678de58032de601d2b04" -- daml-stdlib-DA-Internal-Down
      , "7adc4c2d07fa3a51173c843cba36e610c1168b2dbbf53076e20c0092eae8763d" -- daml-stdlib-DA-Internal-Fail-Types
      , "c280cc3ef501d237efa7b1120ca3ad2d196e089ad596b666bed59a85f3c9a074" -- daml-stdlib-DA-Internal-Interface-AnyView-Types
      , "9e70a8b3510d617f8a136213f33d6a903a10ca0eeec76bb06ba55d1ed9680f69" -- daml-stdlib-DA-Internal-Template
      , "cae345b5500ef6f84645c816f88b9f7a85a9f3c71697984abdf6849f81e80324" -- daml-stdlib-DA-Logic-Types
      , "52854220dc199884704958df38befd5492d78384a032fd7558c38f00e3d778a2" -- daml-stdlib-DA-Monoid-Types
      , "bde4bd30749e99603e5afa354706608601029e225d4983324d617825b634253a" -- daml-stdlib-DA-NonEmpty-Types
      , "bfda48f9aa2c89c895cde538ec4b4946c7085959e031ad61bde616b9849155d7" -- daml-stdlib-DA-Random-Types
      , "d095a2ccf6dd36b2415adc4fa676f9191ba63cd39828dc5207b36892ec350cbc" -- daml-stdlib-DA-Semigroup-Types
      , "c3bb0c5d04799b3f11bad7c3c102963e115cf53da3e4afcbcfd9f06ebd82b4ff" -- daml-stdlib-DA-Set-Types
      , "60c61c542207080e97e378ab447cc355ecc47534b3a3ebbff307c4fb8339bc4d" -- daml-stdlib-DA-Stack-Types
      , "b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946" -- daml-stdlib-DA-Time-Types
      , "3cde94fe9be5c700fc1d9a8ad2277e2c1214609f8c52a5b4db77e466875b8cb7" -- daml-stdlib-DA-Validation-Types
      ]
