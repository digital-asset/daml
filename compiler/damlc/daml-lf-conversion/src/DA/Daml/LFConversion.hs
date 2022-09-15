-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE MultiWayIf #-}
{-# OPTIONS_GHC -Wno-unused-matches #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-} -- Because the pattern match checker is garbage

-- | Useful references:
--
-- * Daml-LF AST: https://github.com/digital-asset/daml/blob/main/compiler/daml-lf-ast/src/DA/Daml/LF/Ast/Base.hs
-- * GHC Syntax: https://hackage.haskell.org/package/ghc-8.4.1/docs/CoreSyn.html#t:Expr
--
-- The conversion works element by element, in a fairly direct way, apart from the exceptions
-- (not all of these need fixing, but all are worth being aware of):
--
-- * Type-defs are expanded everywhere they are used since they aren't in LF.
-- * GHC type class desugaring for default methods relies on lazy top-level
--   bindings. We eta-expand them to avoid the problem. See the comment on
--   DICTIONARY SANITIZATION below.
--
---------------------------------------------------------------------
-- DICTIONARY SANITIZATION
--
-- GHC's desugaring for default methods relies on the fact that Haskell is
-- lazy. In contrast, Daml-LF is strict. This mismatch causes a few problems.
-- For instance, GHC desugars:
--
-- > class Foo a where
-- >   bar :: a -> a
-- >   baz :: a -> a
-- >   baz = bar
-- >
-- > instance Foo Bool where
-- >   bar x = x
--
-- Into:
--
-- > bar = \ (@ a) (v_B1 :: Foo a) ->
--           case v_B1 of v_B1 { C:Foo v_B2 v_B3 -> v_B2 }
-- > baz = \ (@ a) (v_B1 :: Foo a) ->
--           case v_B1 of v_B1 { C:Foo v_B2 v_B3 -> v_B3 }
-- >
-- > $fFooBool = C:Foo @ Bool $cbar $cbaz
-- > $cbar = \ (x :: Bool) -> x
-- > $cbaz = $dmbaz @ Bool $fFooBool
-- > $dmbaz = \ (@ a) ($dFoo :: Foo a) -> bar @ a $dFoo
--
-- If you evaluate @$fFooBool@ eagerly that evaluates @$cbaz@, which evaluates
-- @$fFooBool@ and you loop forever.
--
-- To fix this problem, we make the fields of dictionary types (like @Foo@)
-- lazy by introducing an artificial argument of type @Unit@ to them. In order
-- to do so, we do three transformations on the generated Daml-LF:
--
-- (1) Translate dictionary type definitions with an extra @Unit@ argument
--     for each field.
--
-- (2) Apply an extra @Unit@ argument to each record projection on a
--     dictionary type.
--
-- (3) Wrap every argument to each dictionary type constructor in a lambda
--     introducing a @Unit@.
--
--
--
-- GHC produces a @newtype@ rather than a @data@ type for dictionary types of
-- type classes with a single method and no super classes. Since Daml-LF does
-- not support @newtype@, we could either treat them as some sort of type
-- synonym or translate them to a record type with a single field. We have
-- chosen to do the latter for the sake of uniformity among all dictionary
-- types. GHC Core contains neither constructor nor projection functions for
-- @newtype@s but uses coercions instead. We translate these coercions to
-- proper record construction/projection.

module DA.Daml.LFConversion
    ( convertModule
    , sourceLocToRange
    , convertRationalBigNumeric -- exposed for festing
    , runConvertM -- exposed for testing
    , ConversionEnv(..) -- exposed for testing
    ) where

import           DA.Daml.LFConversion.Primitives
import           DA.Daml.LFConversion.MetadataEncoding
import           DA.Daml.LFConversion.ConvertM
import           DA.Daml.Preprocessor (isInternal)
import           DA.Daml.UtilGHC
import           DA.Daml.UtilLF
import           DA.Pretty (renderPretty)

import           Development.IDE.Types.Diagnostics
import           Development.IDE.Types.Location
import           Development.IDE.GHC.Util

import           Control.Lens hiding (MethodName)
import           Control.Monad.Except
import           Control.Monad.Extra
import           Control.Monad.State.Strict
import           DA.Daml.LF.Ast as LF
import           DA.Daml.LF.Ast.Numeric
import           DA.Daml.LF.TemplateOrInterface (TemplateOrInterface')
import qualified DA.Daml.LF.TemplateOrInterface as TemplateOrInterface
import           DA.Daml.Options.Types (EnableScenarios (..), AllowLargeTuples (..))
import qualified Data.Decimal as Decimal
import           Data.Foldable (foldlM)
import           Data.Int
import           Data.List.Extra
import qualified Data.Map.Strict as MS
import qualified Data.Map.Merge.Strict as MMS
import qualified Data.Set as S
import           Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Text.Extended as T
import           Data.Tuple.Extra
import           Data.Ratio
import           "ghc-lib" GHC
import           "ghc-lib" GhcPlugins as GHC hiding ((<>), notNull)
import           "ghc-lib-parser" InstEnv (ClsInst(..))
import           "ghc-lib-parser" Pair hiding (swap)
import           "ghc-lib-parser" PrelNames
import           "ghc-lib-parser" TysPrim
import           "ghc-lib-parser" TyCoRep
import           "ghc-lib-parser" Class (classHasFds, classMinimalDef, classOpItems)
import qualified "ghc-lib-parser" Name
import qualified "ghc-lib-parser" Avail as GHC
import qualified "ghc-lib-parser" BooleanFormula as BF
import           Safe.Exact (zipExact, zipExactMay)
import           SdkVersion

---------------------------------------------------------------------
-- FUNCTIONS ON THE ENVIRONMENT

data Env = Env
    {envLFModuleName :: LF.ModuleName
    ,envGHCModuleName :: GHC.ModuleName
    ,envModuleUnitId :: GHC.UnitId
    ,envAliases :: MS.Map Var LF.Expr
    ,envPkgMap :: MS.Map GHC.UnitId LF.DalfPackage
    ,envStablePackages :: MS.Map (GHC.UnitId, LF.ModuleName) LF.PackageId
    -- This mapping allows us to split individual modules out from a GHC package into their
    -- own LF package. Thereby we can guarantee stable package ids at the LF level without having
    -- to rely on data-dependencies to generate interface files.
    -- Once data dependencies are well-supported we might want to remove this if the number of GHC
    -- packages does not cause performance issues.
    ,envLfVersion :: LF.Version
    ,envEnableScenarios :: EnableScenarios
    ,envAllowLargeTuples :: AllowLargeTuples
    ,envTypeVars :: !(MS.Map Var TypeVarName)
        -- ^ Maps GHC type variables in scope to their LF type variable names
    ,envTypeVarNames :: !(S.Set TypeVarName)
        -- ^ The set of LF type variable names in scope (i.e. the set of
        -- values of 'envTypeVars').
    }

mkEnv ::
     LF.Version
  -> EnableScenarios
  -> AllowLargeTuples
  -> MS.Map UnitId DalfPackage
  -> MS.Map (UnitId, LF.ModuleName) PackageId
  -> GHC.Module
  -> Env
mkEnv envLfVersion envEnableScenarios envAllowLargeTuples envPkgMap envStablePackages ghcModule = do
  let
    envGHCModuleName = GHC.moduleName ghcModule
    envModuleUnitId = GHC.moduleUnitId ghcModule
    envLFModuleName = convertModuleName envGHCModuleName
    envAliases = MS.empty
    envTypeVars = MS.empty
    envTypeVarNames = S.empty
  Env {..}

-- v is an alias for x
envInsertAlias :: Var -> LF.Expr -> Env -> Env
envInsertAlias v x env = env{envAliases = MS.insert v x (envAliases env)}

envLookupAlias :: Var -> Env -> Maybe LF.Expr
envLookupAlias x = MS.lookup x . envAliases

-- | Get a fresh type var whose LF name has not yet been taken.
envFreshTypeVar :: T.Text -> Env -> (TypeVarName, Env)
envFreshTypeVar nameHint env = try 1 (TypeVarName nameHint)
    where
        nameFor i = TypeVarName (nameHint <> T.pack (show i))

        try :: Int -> TypeVarName -> (TypeVarName, Env)
        try !i name =
            if envHasTypeVarName name env
                then try (i+1) (nameFor i)
                else (name, env{envTypeVarNames = S.insert name (envTypeVarNames env)})

-- | Bind a type var without shadowing its LF name.
envBindTypeVar :: Var -> Env -> (TypeVarName, Env)
envBindTypeVar x env0 =
    let (n, env) = envFreshTypeVar (getOccText x) env0 in
    (n, env{envTypeVars = MS.insert x n (envTypeVars env)})

-- | Bind multiple type vars without shadowing their LF names.
envBindTypeVars :: [Var] -> Env -> ([TypeVarName], Env)
envBindTypeVars xs env = foldr f ([], env) xs
    where
        f x (ys, env) =
            let (y, env') = envBindTypeVar x env in (y:ys, env')

envLookupTypeVar :: Var -> Env -> Maybe TypeVarName
envLookupTypeVar x = MS.lookup x . envTypeVars

envHasTypeVarName :: TypeVarName -> Env -> Bool
envHasTypeVarName x = S.member x . envTypeVarNames

---------------------------------------------------------------------
-- EXTRACTION

-- | Information extracted from the GHC representation of the module as an
-- initial step of conversion.
data ModuleContents = ModuleContents
  { mcBinds :: [(Var, GHC.Expr CoreBndr)]
  , mcTypeDefs :: [TyThing]
  , mcTemplateBinds :: MS.Map TypeConName TemplateBinds
  , mcExceptionBinds :: MS.Map TypeConName ExceptionBinds
  , mcInterfaceBinds :: MS.Map TypeConName InterfaceBinds
    -- ^ Maps an interface to the contents of its definition.
  , mcInterfaceInstanceBinds :: MS.Map TypeConName InterfaceInstanceGroup
    -- ^ Maps a template to the interface instances defined in it.
  , mcChoiceData :: MS.Map TypeConName [ChoiceData]
  , mcModInstanceInfo :: !ModInstanceInfo
  , mcDepOrphanModules :: [GHC.Module]
  , mcExports :: [GHC.AvailInfo]
  , mcFixities :: [(OccName, GHC.Fixity)]
  }

data ChoiceData = ChoiceData
  { _choiceDatTy :: GHC.Type
  , _choiceDatExpr :: GHC.Expr GHC.CoreBndr
  }

extractModuleContents :: Env -> CoreModule -> ModIface -> ModDetails -> ModuleContents
extractModuleContents env@Env{..} coreModule modIface details = do
  let
    mcBinds =
      [ bind
      | bindGroup <- cm_binds coreModule
      , bind <- case bindGroup of
          NonRec name body
            -- NOTE(MH): We can't cope with the generated Typeable stuff, so remove those bindings
            | any (`T.isPrefixOf` getOccText name) ["$krep", "$tc", "$trModule"] -> []
            | otherwise -> [(name, body)]
          Rec binds -> binds
      ]
    mcTypeDefs = eltsUFM (cm_types coreModule)
    mcInterfaceBinds = scrapeInterfaceBinds envLfVersion mcTypeDefs mcBinds
    mcInterfaceInstanceBinds = scrapeInterfaceInstanceBinds env mcBinds
    mcChoiceData = MS.fromListWith (++)
        [ (mkTypeCon [getOccText tplTy], [ChoiceData ty v])
        | (name, v) <- mcBinds
        , "_choice_" `T.isPrefixOf` getOccText name
        , ty@(TypeCon _ [_, _, TypeCon _ [TypeCon tplTy _], _]) <- [varType name]
        ]
    mcTemplateBinds = scrapeTemplateBinds mcBinds
    mcExceptionBinds
        | envLfVersion `supports` featureExceptions =
            scrapeExceptionBinds mcBinds
        | otherwise =
            MS.empty

    mcModInstanceInfo = modInstanceInfoFromDetails details
    mcDepOrphanModules = getDepOrphanModules modIface
    mcExports = md_exports details
    mcFixities = mi_fixities modIface

  ModuleContents {..}

getDepOrphanModules :: ModIface -> [GHC.Module]
getDepOrphanModules = dep_orphs . mi_deps

---------------------------------------------------------------------
-- CONVERSION

convertInt64 :: Integer -> ConvertM LF.Expr
convertInt64 x
    | toInteger (minBound :: Int64) <= x && x <= toInteger (maxBound :: Int64) =
        pure $ EBuiltin $ BEInt64 (fromInteger x)
    | otherwise =
        unsupported "Int literal out of bounds." (negate x)

-- | Convert a rational number into a (legacy) Decimal literal.
convertRationalDecimal :: Env -> Integer -> Integer -> ConvertM LF.Expr
convertRationalDecimal env num denom
 =
    -- the denominator needs to be a divisor of 10^10.
    -- num % denom * 10^10 needs to fit within a 128bit signed number.
    -- note that we can also get negative rationals here, hence we ask for upperBound128Bit - 1 as
    -- upper limit.
    if 10 ^ maxPrecision `mod` denom == 0 && abs (r * 10 ^ maxPrecision) <= upperBound128Bit - 1 then
        pure $ EBuiltin $
          BENumeric $ numericFromDecimal $ fromRational r
    else
        unsupported
            ("Rational is out of bounds: " ++
             show ((fromInteger num / fromInteger denom) :: Double) ++
             ".  Maximal supported precision is e^-10, maximal range after multiplying with 10^10 is [10^38 -1, -10^38 + 1].")
            (num, denom)
  where
    r = num % denom
    upperBound128Bit = 10 ^ (38 :: Integer)
    maxPrecision = 10 :: Integer

-- | Convert a rational number into a fixed scale Numeric literal. We check
-- that the scale is in bound, and the number can be represented without
-- overflow or loss of precision.
convertRationalNumericMono :: Env -> Integer -> Integer -> Integer -> ConvertM LF.Expr
convertRationalNumericMono env scale num denom
    | scale < 0 || scale > fromIntegral numericMaxScale =
        unsupported
            ("Tried to construct value of type Numeric " ++ show scale ++ ", but scale is out of bounds. Scale must be between 0 through 37, not " ++ show scale ++ ".")
            scale

    | abs (rational * 10 ^ scale) >= 10 ^ numericMaxPrecision =
        unsupported
            ("Rational is out of bounds: " ++ show double ++ ". The Numeric " ++ show scale ++ " type can only represent numbers greater than -10^" ++ show maxPower ++ " and smaller than 10^" ++ show maxPower ++ ".")
            (num, denom)

    | (num * 10^scale) `mod` denom /= 0 =
        unsupported
            ("Rational is out of bounds: " ++ show double ++ ". It cannot be represented without loss of precision. Maximum precision for the Numeric " ++ show scale ++ " type is 10^-" ++ show scale ++ ".")
            (num, denom)

    | otherwise =
        pure $ EBuiltin $ BENumeric $
            numeric (fromIntegral scale)
                    ((num * 10^scale) `div` denom)

    where
        rational = num % denom
        double = (fromInteger num / fromInteger denom) :: Double
        maxPower = fromIntegral numericMaxPrecision - scale

-- | Convert a rational into a BigNumeric expression. Currently only supports
-- values that will fit in a Numeric.
convertRationalBigNumeric :: Integer -> Integer -> ConvertM LF.Expr
convertRationalBigNumeric num denom = case numericFromRational rational of
    Left _ -> invalid
    Right n ->
        let scale = numericScale n
        in pure (EBuiltin BENumericToBigNumeric
            `ETyApp` TNat (typeLevelNat scale)
            `ETmApp` EBuiltin (BENumeric n))

    where
        rational = num % denom
        invalid = unsupported "Large BigNumeric (larger than Numeric) literals are not currently supported. Please construct the number from smaller literals." ()

data TemplateBinds = TemplateBinds
    { tbTyCon :: Maybe GHC.TyCon
    , tbSignatory :: Maybe (GHC.Expr Var)
    , tbEnsure :: Maybe (GHC.Expr Var)
    , tbAgreement :: Maybe (GHC.Expr Var)
    , tbObserver :: Maybe (GHC.Expr Var)
    , tbArchive :: Maybe (GHC.Expr Var)
    , tbKeyType :: Maybe GHC.Type
    , tbKey :: Maybe (GHC.Expr Var)
    , tbMaintainer :: Maybe (GHC.Expr Var)
    , tbShow :: Maybe GHC.Var
    }

emptyTemplateBinds :: TemplateBinds
emptyTemplateBinds = TemplateBinds
    Nothing Nothing Nothing Nothing Nothing Nothing
    Nothing Nothing Nothing Nothing

scrapeTemplateBinds :: [(Var, GHC.Expr Var)] -> MS.Map TypeConName TemplateBinds
scrapeTemplateBinds binds = MS.filter (isJust . tbTyCon) $ MS.map ($ emptyTemplateBinds) $ MS.fromListWith (.)
    [ (mkTypeCon [getOccText (GHC.tyConName tpl)], fn)
    | (name, expr) <- binds
    , Just (tpl, fn) <- pure $ case name of
        HasSignatoryDFunId tpl ->
            Just (tpl, \tb -> tb { tbTyCon = Just tpl, tbSignatory = Just expr })
        HasEnsureDFunId tpl ->
            Just (tpl, \tb -> tb { tbEnsure = Just expr })
        HasAgreementDFunId tpl ->
            Just (tpl, \tb -> tb { tbAgreement = Just expr })
        HasObserverDFunId tpl ->
            Just (tpl, \tb -> tb { tbObserver = Just expr })
        HasArchiveDFunId tpl ->
            Just (tpl, \tb -> tb { tbArchive = Just expr })
        HasKeyDFunId tpl key ->
            Just (tpl, \tb -> tb { tbKeyType = Just key, tbKey = Just expr })
        HasMaintainerDFunId tpl _key ->
            Just (tpl, \tb -> tb { tbMaintainer = Just expr })
        ShowDFunId tpl ->
            Just (tpl, \tb -> tb { tbShow = Just name })
        _ -> Nothing
    , hasDamlTemplateCtx tpl
    ]

data ExceptionBinds = ExceptionBinds
    { ebTyCon :: GHC.TyCon
    , ebMessage :: GHC.Expr Var
    }

scrapeExceptionBinds :: [(Var, GHC.Expr Var)] -> MS.Map TypeConName ExceptionBinds
scrapeExceptionBinds binds = MS.fromList
    [ ( mkTypeCon [getOccText (GHC.tyConName exn)]
      , ExceptionBinds { ebTyCon = exn, ebMessage = msg } )
    | (HasMessageDFunId exn, msg) <- binds
    , hasDamlExceptionCtx exn
    ]

type ModInstanceInfo = MS.Map DFunId OverlapMode

modInstanceInfoFromDetails :: ModDetails -> ModInstanceInfo
modInstanceInfoFromDetails ModDetails{..} = MS.fromList
    [ (is_dfun, overlapMode is_flag) | ClsInst{..} <- md_insts ]

-- | Represents the contents of some interface instance
data InterfaceBinds = InterfaceBinds
  { ibTyCon :: TyCon
      -- ^ Type constructor associated to the interface declaration. Its location
      -- should point to the @interface X@ line in the daml file.
  , ibViewType :: Maybe GHC.Type
      -- ^ The view type associated with this interface.
  , ibMethods :: MS.Map MethodName (GHC.Type, Maybe SourceLoc)
      -- ^ The methods defined in this interface, together with their types
      -- and the location of the definition.
  , ibRequires :: [(GHC.TyCon, Maybe SourceLoc)]
      -- ^ The interfaces required by this interface.
  }

emptyInterfaceBinds :: TyCon -> InterfaceBinds
emptyInterfaceBinds ibTyCon = InterfaceBinds
  { ibTyCon
  , ibViewType = Nothing
  , ibMethods = MS.empty
  , ibRequires = []
  }

setInterfaceViewType :: GHC.Type -> InterfaceBinds -> InterfaceBinds
setInterfaceViewType viewType ib = ib
  { ibViewType = Just viewType
  }

insertInterfaceMethod ::
     MethodName
  -> GHC.Type
  -> Maybe SourceLoc
  -> InterfaceBinds
  -> InterfaceBinds
insertInterfaceMethod methodName retTy loc ib = ib
  { ibMethods = MS.insert methodName (retTy, loc) (ibMethods ib)
  }

insertInterfaceRequires :: GHC.TyCon -> Maybe SourceLoc -> InterfaceBinds -> InterfaceBinds
insertInterfaceRequires required loc ib = ib
  { ibRequires = (required, loc) : ibRequires ib
  }

scrapeInterfaceBinds ::
     LF.Version
  -> [TyThing]
  -> [(Var, GHC.Expr Var)]
  -> MS.Map TypeConName InterfaceBinds
scrapeInterfaceBinds lfVersion tyThings binds =
  MMS.merge
    {- drop bind funcs without interfaces -}
    MMS.dropMissing
    {- keep interfaces without bind funcs -}
    MMS.preserveMissing'
    {- apply bind funcs to interfaces -}
    (MMS.zipWithMatched (const ($!)))
    interfaceBindFs
    interfaces
  where
    interfaces :: MS.Map TypeConName InterfaceBinds
    interfaces = MS.fromList
      [ (mkTypeCon [getOccText t], emptyInterfaceBinds t)
      | ATyCon t <- tyThings
      , hasDamlInterfaceCtx t
      ]

    interfaceBindFs :: MS.Map TypeConName (InterfaceBinds -> InterfaceBinds)
    interfaceBindFs = MS.fromListWith (.)
      [ (mkTypeCon [getOccText interface], fn)
      | (name, expr) <- binds
      , Just (interface, fn) <- pure $ case name of
          HasInterfaceViewDFunId interface viewType ->
            Just (interface, setInterfaceViewType viewType)
          HasMethodDFunId interface methodName retTy ->
            Just (interface, insertInterfaceMethod methodName retTy (convNameLoc name))
          name
            | "_requires_" `T.isPrefixOf` getOccText name
            , TypeCon requiresT [TypeCon iface1 [], TypeCon iface2 []] <- varType name
            , NameIn DA_Internal_Desugar "RequiresT" <- requiresT
            -> Just (iface1, insertInterfaceRequires iface2 (convNameLoc name))
          _ -> Nothing
      ]

-- | Represents an interface instance, including the interface + template pair
-- for which it is defined. Different 'InterfaceInstanceGroup's might contain
-- 'InterfaceInstanceBinds's with the same interface + template pair, but
-- we don't care during conversion, leaving it to the typechecker.
data InterfaceInstanceBinds = InterfaceInstanceBinds
  { iibInterface :: GHC.TyCon
  , iibTemplate :: GHC.TyCon
  , iibLoc :: Maybe SourceLoc
      -- ^ Location associated to the @_interface_instance_@ marker, which should
      -- point to the @interface instance@ line in the daml file.
  , iibMethods :: MS.Map MethodName (GHC.Expr GHC.CoreBndr)
      -- ^ Method implementations.
  , iibView :: [GHC.Expr GHC.CoreBndr]
      -- ^ View implementation.
  }

-- | A group of interface instances under a certain template.
newtype InterfaceInstanceGroup = InterfaceInstanceGroup
  { iigMap :: MS.Map (Name, Name) InterfaceInstanceBinds
      -- ^ The keys should be @(GHC.TyCon, GHC.TyCon)@, but there's no
      -- @instance Ord GHC.TyCon@. Instead, we use @(Name, Name)@ and
      -- apply 'iigKey' when necessary.
  }

iigKey :: GHC.TyCon -> GHC.TyCon -> (Name, Name)
iigKey interface template =
  (getName interface, getName template)

iigAdjust ::
     GHC.TyCon
  -> GHC.TyCon
  -> (InterfaceInstanceBinds -> InterfaceInstanceBinds)
  -> (InterfaceInstanceGroup -> InterfaceInstanceGroup)
iigAdjust interface template f =
  InterfaceInstanceGroup . MS.adjust f (iigKey interface template) . iigMap

iigUnion :: InterfaceInstanceGroup -> InterfaceInstanceGroup -> InterfaceInstanceGroup
iigUnion g h = InterfaceInstanceGroup (MS.union (iigMap g) (iigMap h))

interfaceInstanceGroupBinds :: InterfaceInstanceGroup -> [InterfaceInstanceBinds]
interfaceInstanceGroupBinds = MS.elems . iigMap

singletonInterfaceInstanceGroup ::
     GHC.TyCon
  -> GHC.TyCon
  -> Maybe SourceLoc
  -> InterfaceInstanceGroup
singletonInterfaceInstanceGroup iibInterface iibTemplate iibLoc =
  InterfaceInstanceGroup (MS.singleton (iigKey iibInterface iibTemplate) iib)
  where
    iib = InterfaceInstanceBinds
      { iibInterface
      , iibTemplate
      , iibLoc
      , iibMethods = MS.empty
      , iibView = []
      }

insertInterfaceInstanceView ::
     GHC.TyCon
  -> GHC.TyCon
  -> GHC.Expr GHC.CoreBndr
  -> InterfaceInstanceGroup
  -> InterfaceInstanceGroup
insertInterfaceInstanceView interface template view =
  iigAdjust interface template
    (\iib -> iib { iibView = view : iibView iib })

insertInterfaceInstanceMethod ::
     GHC.TyCon
  -> GHC.TyCon
  -> MethodName
  -> GHC.Expr GHC.CoreBndr
  -> InterfaceInstanceGroup
  -> InterfaceInstanceGroup
insertInterfaceInstanceMethod interface template methodName methodExpr =
  iigAdjust interface template
    (\iib -> iib { iibMethods = MS.insert methodName methodExpr (iibMethods iib) })

scrapeInterfaceInstanceBinds ::
     Env
  -> [(Var, GHC.Expr CoreBndr)]
  -> MS.Map TypeConName InterfaceInstanceGroup
scrapeInterfaceInstanceBinds env binds =
  MMS.merge
    {- drop group funcs without interface instances -}
    MMS.dropMissing
    {- keep interface instances without group funcs -}
    MMS.preserveMissing'
    {- apply group funcs to interface instances -}
    (MMS.zipWithMatched (const ($!)))
    interfaceInstanceGroupFs
    interfaceInstanceGroups
  where
    interfaceInstanceGroups :: MS.Map TypeConName InterfaceInstanceGroup
    interfaceInstanceGroups = MS.fromListWith iigUnion
      [ ( mkTypeCon [getOccText parent]
        , singletonInterfaceInstanceGroup interface template (convNameLoc name)
        )
      | (name, _val) <- binds
      , "_interface_instance_" `T.isPrefixOf` getOccText name
      , TypeCon (NameIn DA_Internal_Desugar "InterfaceInstance")
          [ TypeCon parent []
          , TypeCon interface []
          , TypeCon template []
          ] <- [varType name]
      ]

    interfaceInstanceGroupFs ::
      MS.Map TypeConName (InterfaceInstanceGroup -> InterfaceInstanceGroup)
    interfaceInstanceGroupFs = MS.fromListWith (.)
      [ (mkTypeCon [getOccText parent], fn)
      | (name, untick -> expr) <- binds
      , Just (parent, fn) <- pure $ case name of
          name
            | TypeCon (NameIn DA_Internal_Desugar "InterfaceView")
                [ TypeCon parent []
                , TypeCon interface []
                , TypeCon template []
                ] <- varType name
            -> Just (parent, insertInterfaceInstanceView interface template expr)
          name
            | TypeCon (NameIn DA_Internal_Desugar "Method")
                [ TypeCon parent []
                , TypeCon interface []
                , TypeCon template []
                , StrLitTy (MethodName -> methodName)
                ] <- varType name
            -> Just (parent, insertInterfaceInstanceMethod interface template methodName expr)
          _ -> Nothing
      ]

convertDamlTyCon ::
     (TyCon -> Bool)
  -> String
  -> Env
  -> (GHC.TyCon -> String)
  -> GHC.TyCon
  -> ConvertM (LF.Qualified LF.TypeConName)
convertDamlTyCon hasExpectedCtx unhandledStr env errHandler tycon
    | hasExpectedCtx tycon = do
        lfType <- convertTyCon env tycon
        case lfType of
            TCon con -> pure con
            _ -> unhandled unhandledStr tycon
    | otherwise =
        conversionError $ errHandler tycon

convertInterfaceTyCon :: Env -> (GHC.TyCon -> String) -> GHC.TyCon -> ConvertM (LF.Qualified LF.TypeConName)
convertInterfaceTyCon = convertDamlTyCon hasDamlInterfaceCtx "interface type"

convertTemplateTyCon :: Env -> (GHC.TyCon -> String) -> GHC.TyCon -> ConvertM (LF.Qualified LF.TypeConName)
convertTemplateTyCon = convertDamlTyCon hasDamlTemplateCtx "template type"

convertInterfaces :: Env -> ModuleContents -> ConvertM [Definition]
convertInterfaces env mc =
  concatMapM
    (\(name, binds) -> convertInterface env mc name binds)
    (MS.toList (mcInterfaceBinds mc))

convertInterface :: Env -> ModuleContents -> LF.TypeConName -> InterfaceBinds -> ConvertM [Definition]
convertInterface env mc intName ib =
  withRange intLocation do
    unless (envLfVersion env `supports` featureSimpleInterfaces) do
      unsupported "Daml interfaces are only available with --target=1.15 or higher" ()
    defInterfaceDataType <- convertDefInterfaceDataType
    defInterface <- convertDefInterface
    pure
      [ defInterfaceDataType
      , defInterface
      ]
  where
    tyCon = ibTyCon ib
    intLocation = convNameLoc tyCon

    convertDefInterfaceDataType :: ConvertM Definition
    convertDefInterfaceDataType = do
      unless (null (tyConTyVars tyCon)) do
        unhandled "interface type constructor with type parameters" tyCon
      pure $ DDataType DefDataType
        { dataLocation = Nothing
        , dataTypeCon = intName
        , dataSerializable = IsSerializable False
        , dataParams = []
        , dataCons = DataInterface
        }

    convertDefInterface :: ConvertM Definition
    convertDefInterface = do
      let
        intParam = this
      intRequires <- convertRequires (ibRequires ib)
      intMethods <- convertMethods (ibMethods ib)
      intChoices <- convertChoices env mc intName emptyTemplateBinds
      intCoImplements <- convertCoImplements intName
      intView <- case ibViewType ib of
          Nothing -> conversionError $ "No view found for interface " <> renderPretty intName
          Just viewType -> convertType env viewType
      pure $ DInterface DefInterface {..}

    convertRequires :: [(GHC.TyCon, Maybe SourceLoc)] -> ConvertM (S.Set (Qualified TypeConName))
    convertRequires requires = S.fromList <$>
      forM requires \(iface, mloc) ->
        withRange mloc do
          unless (envLfVersion env `supports` featureExtendedInterfaces) do
            unsupported "Requires in Daml interfaces are only available with --target=1.dev" ()
          convertInterfaceTyCon env handleIsNotInterface iface
      where
        handleIsNotInterface tyCon =
          "cannot require '" ++ prettyPrint tyCon ++ "' because it is not an interface"

    convertMethods ::
         MS.Map MethodName (GHC.Type, Maybe SourceLoc)
      -> ConvertM (NM.NameMap InterfaceMethod)
    convertMethods methods =
      NM.fromList <$> sequence
        [ withRange loc $ do
            retTy' <- convertType env retTy
            pure InterfaceMethod
              { ifmLocation = loc
              , ifmName = methodName
              , ifmType = retTy'
              }
        | (methodName, (retTy, loc)) <- MS.toList methods
        ]

    convertCoImplements :: LF.TypeConName -> ConvertM (NM.NameMap InterfaceCoImplements)
    convertCoImplements interface = NM.fromList <$>
      mapM convertCoImplements1
        (maybe [] interfaceInstanceGroupBinds (MS.lookup interface (mcInterfaceInstanceBinds mc)))
      where
        convertCoImplements1 :: InterfaceInstanceBinds -> ConvertM InterfaceCoImplements
        convertCoImplements1 =
          convertInterfaceInstance
            (TemplateOrInterface.Interface interface)
            (\_ template -> InterfaceCoImplements template)
            env

convertConsuming :: LF.Type -> ConvertM Consuming
convertConsuming consumingTy = case consumingTy of
      TConApp Qualified { qualObject = TypeConName con } _
          | con == ["NonConsuming"] -> pure NonConsuming
          | con == ["PreConsuming"] -> pure PreConsuming
          | con == ["Consuming"] -> pure Consuming
          | con == ["PostConsuming"] -> pure PostConsuming
      _ -> unhandled "choice consumption type" (show consumingTy)

convertModule
    :: LF.Version
    -> EnableScenarios
    -> AllowLargeTuples
    -> MS.Map UnitId DalfPackage
    -> MS.Map (GHC.UnitId, LF.ModuleName) LF.PackageId
    -> NormalizedFilePath
    -> CoreModule
    -> ModIface
      -- ^ Only used for information that isn't available in ModDetails.
    -> ModDetails
    -> Either FileDiagnostic (LF.Module, [FileDiagnostic])
convertModule lfVersion enableScenarios allowLargeTuples pkgMap stablePackages file coreModule modIface details = runConvertM (ConversionEnv file Nothing) $ do
    let
      env = mkEnv lfVersion enableScenarios allowLargeTuples pkgMap stablePackages (cm_module coreModule)
      mc = extractModuleContents env coreModule modIface details
    defs <- convertModuleContents env mc
    pure (LF.moduleFromDefinitions (envLFModuleName env) (Just $ fromNormalizedFilePath file) flags defs)
    where
        flags = LF.daml12FeatureFlags

convertModuleContents :: Env -> ModuleContents -> ConvertM [Definition]
convertModuleContents env mc = do
    definitions <- convertBinds env mc
    types <- convertTypeDefs env mc
    depOrphanModules <- convertDepOrphanModules env mc
    templates <- convertTemplateDefs env mc
    exceptions <- convertExceptionDefs env mc
    interfaces <- convertInterfaces env mc
    exports <- convertExports env mc
    let fixities = convertFixities mc
        defs =
            types
            ++ templates
            ++ exceptions
            ++ definitions
            ++ interfaces
            ++ depOrphanModules
            ++ exports
            ++ fixities
    pure defs

data Consuming = PreConsuming
               | Consuming
               | NonConsuming
               | PostConsuming
               deriving (Eq)

convertTypeDefs :: Env -> ModuleContents -> ConvertM [Definition]
convertTypeDefs env mc = concatMapM (convertTypeDef env) (mcTypeDefs mc)

convertTypeDef :: Env -> TyThing -> ConvertM [Definition]
convertTypeDef env o@(ATyCon t) = withRange (convNameLoc t) $ if
    -- Internal types (i.e. already defined in LF)
    | NameIn DA_Internal_LF n <- t
    , n `elementOfUniqSet` internalTypes
    -> pure []
    | NameIn DA_Internal_Prelude "Optional" <- t -> pure []
    -- Types used only for desugaring are dropped during the LF conversion.
    | NameIn DA_Internal_Desugar n <- t
    , n `elementOfUniqSet` desugarTypes
    -> pure []

    -- The type declarations for interfaces are generated by 'convertInterface'
    | hasDamlInterfaceCtx t
    -> pure []

    -- Remove guarded exercise instances when Extended Interfaces are unsupported
    | not (envLfVersion env `supports` featureExtendedInterfaces)
    , Just cls <- tyConClass_maybe t
    , NameIn DA_Internal_Template_Functions "HasExerciseGuarded" <- cls
    ->  pure []

    -- Constraint tuples are represented by LF structs.
    | isConstraintTupleTyCon t
    -> pure []

    -- Enum types. These are algebraic types without any type arguments,
    -- with two or more constructors that have no arguments.
    | isEnumTyCon t
    -> convertEnumDef env t

    -- Type classes
    | isClassTyCon t
    -> convertClassDef env t

    -- Type synonyms get expanded out during conversion (see 'convertType'), but we also
    -- convert the synonyms we can so that we can expose them via data-dependencies.
    | isTypeSynonymTyCon t
    -> convertTypeSynonym env t

    -- Simple record types. This includes newtypes, and
    -- single constructor algebraic types with no fields or with
    -- labelled fields.
    | isSimpleRecordTyCon t
    -> convertSimpleRecordDef env t

    -- Variants are algebraic types that are not enums and not simple
    -- record types. This includes most 'data' types.
    | isVariantTyCon t
    -> convertVariantDef env t

    | otherwise
    -> unsupported ("Data definition, of type " ++ prettyPrint (tyConFlavour t) ++ ".") o

convertTypeDef env x = pure []

convertEnumDef :: Env -> TyCon -> ConvertM [Definition]
convertEnumDef env t =
    pure [defDataType tconName [] $ DataEnum ctorNames]
  where
    tconName = mkTypeCon [getOccText t]
    ctorNames = map (mkVariantCon . getOccText) (tyConDataCons t)

convertSimpleRecordDef :: Env -> TyCon -> ConvertM [Definition]
convertSimpleRecordDef env tycon = do
    let con = tyConSingleDataCon tycon
        flavour = tyConFlavour tycon
        labels = ctorLabels con
        (_, theta, args, _) = dataConSig con

    (env', tyVars) <- bindTypeVars env (tyConTyVars tycon)
    fieldTypes <- mapM (convertType env') (theta ++ args)

    let fields = zipExact labels fieldTypes
        tconName = mkTypeCon [getOccText tycon]
        typeDef = defDataType tconName tyVars (DataRecord fields)
        workerDef = defNewtypeWorker env tycon tconName con tyVars fields

    pure $ typeDef : [workerDef | flavour == NewtypeFlavour]

convertTypeSynonym :: Env -> TyCon -> ConvertM [Definition]
convertTypeSynonym env tycon
    | Just (params, body) <- synTyConDefn_maybe tycon
    , not (isKindTyCon tycon)
    = do
        let isConstraintSynonym =
                maybe False (isClassTyCon . fst) (splitTyConApp_maybe body)
            tsynName = mkTypeSyn [getOccText tycon]
        tsynKind <- convertKind $ tyConResKind tycon
        (env', tsynParams) <- bindTypeVars env params
        tsynType <- convertType env' body
        case encodeTypeSynonym tsynName isConstraintSynonym tsynKind tsynParams tsynType of
            DefTypeSyn { synType = TUnit } -> pure []
                -- We avoid converting TUnit type synonyms because it
                -- clashes with the conversion of empty typeclasses.
            def -> pure [DTypeSyn def]
    | otherwise
    = pure []

convertClassDef :: Env -> TyCon -> ConvertM [Definition]
convertClassDef env tycon
    | Just cls <- tyConClass_maybe tycon
    = do
    let con = tyConSingleDataCon tycon
        sanitize = (TUnit :->) -- DICTIONARY SANITIZATION step (1)
        labels = ctorLabels con
        (_, theta, args, _) = dataConSig con

    (env', tyVars) <- bindTypeVars env (tyConTyVars tycon)
    fieldTypes <- mapM (convertType env') (theta ++ args)

    let fields = zipExact labels (map sanitize fieldTypes)
        tsynName = mkTypeSyn [getOccText tycon]
        typeDef = defTypeSyn tsynName tyVars (if null fields then TUnit else TStruct fields)

    let funDeps = snd (classTvsFds cls)
    funDeps' <- mapM (mapFunDepM (convTypeVarName env')) funDeps

    let funDepTyVars = [(v, KStar) | (v, _) <- tyVars]
            -- We use the the type variables as types in the fundep encoding,
            -- not as whatever kind they were previously defined.
        funDepType = TForalls funDepTyVars (encodeFunDeps funDeps')
        funDepDef = DValue (mkMetadataStub (funDepName tsynName) funDepType)

    let minimal = fmap getOccText (classMinimalDef cls)
        methodsWithNoDefault = sort [ getOccText id | (id, Nothing) <- classOpItems cls ]
            -- Used when MINIMAL pragma is not given,
            -- i.e. the minimal sig is all methods without a default implementation.
        minimalIsDefault =
            case minimal of
                BF.Var x -> [x] == methodsWithNoDefault
                BF.And subclauses
                    | let names = [ name | BF.Var name <- map unLoc subclauses ]
                    , length names == length subclauses
                    -> sort names == methodsWithNoDefault
                _ -> False
        minimalType = encodeBooleanFormula minimal
        minimalDef = DValue (mkMetadataStub (minimalName tsynName) minimalType)

    pure $ [typeDef]
        ++ [funDepDef | classHasFds cls]
        ++ [minimalDef | not minimalIsDefault]

convertDepOrphanModules :: Env -> ModuleContents -> ConvertM [Definition]
convertDepOrphanModules env mc = do
    qualifiedDepOrphanModules <- S.fromList <$>
        mapM (convertQualifiedModuleName () env) (mcDepOrphanModules mc)
    let moduleImportsType = encodeModuleImports qualifiedDepOrphanModules
        moduleImportsDef = DValue (mkMetadataStub moduleImportsName moduleImportsType)
    pure [moduleImportsDef]

convertFixities :: ModuleContents -> [Definition]
convertFixities = zipWith mkFixityDef [0..] . mcFixities
  where
    mkFixityDef i fixityInfo =
      DValue $
        mkMetadataStub
          (fixityName i)
          (encodeFixityInfo fixityInfo)

convertExports :: Env -> ModuleContents -> ConvertM [Definition]
convertExports env mc = do
    let externalExportInfos = filter isExternalAvailInfo (mcExports mc)
    exportInfos <- mapM availInfoToExportInfo externalExportInfos
    pure $ zipWith mkExportDef [0..] exportInfos
    where
        isExternalAvailInfo :: GHC.AvailInfo -> Bool
        isExternalAvailInfo = isExternalName . GHC.availName
            where
                isExternalName name =
                    not $
                        nameIsLocalOrFrom thisModule name
                        || isSystemName name
                        || isWiredInName name
                        || maybe False (isInternal . GHC.moduleName) (nameModule_maybe name)
                thisModule = GHC.Module (envModuleUnitId env) (envGHCModuleName env)

        availInfoToExportInfo :: GHC.AvailInfo -> ConvertM ExportInfo
        availInfoToExportInfo = \case
            GHC.Avail name -> ExportInfoVal
                <$> convertQualName name
            GHC.AvailTC name pieces fields -> ExportInfoTC
                <$> convertQualName name
                <*> mapM convertQualName (filter (/= name) pieces)
                    -- NOTE (MA): 'pieces' includes an entry for the type/class,
                    -- but we exclude it since otherwise the export list entry
                    -- would include it as a child, e.g.
                    -- for
                    --   module Type
                    --   data T = MkT
                    -- we'd get
                    --   module Type     (Type.T (Type.T, Type.MkT)) where
                    -- with an error reported for this ^
                <*> mapM convertFieldLabel fields

        convertQualName :: GHC.Name -> ConvertM QualName
        convertQualName = fmap QualName . convertQualified getOccName env

        convertFieldLabel :: GHC.FieldLabel -> ConvertM (GHC.FieldLbl QualName)
        convertFieldLabel f = do
            flSelector <- convertQualName (flSelector f)
            pure f { flSelector }

        mkExportDef :: Integer -> ExportInfo -> Definition
        mkExportDef i info =
            let exportType = encodeExportInfo info
            in DValue (mkMetadataStub (exportName i) exportType)

defNewtypeWorker :: NamedThing a => Env -> a -> TypeConName -> DataCon
    -> [(TypeVarName, LF.Kind)] -> [(FieldName, LF.Type)] -> Definition
defNewtypeWorker env loc tconName con tyVars fields =
    let tcon = TypeConApp
            (qualifyLocally env tconName)
            (map (TVar . fst) tyVars)
        workerName = mkWorkerName (getOccText con)
        workerType = mkTForalls tyVars $ mkTFuns (map snd fields) $ typeConAppToType tcon
        workerBody = mkETyLams tyVars $ mkETmLams (map (first fieldToVar) fields) $
            ERecCon tcon [(label, EVar (fieldToVar label)) | (label,_) <- fields]
    in defValue loc (workerName, workerType) workerBody

convertVariantDef :: Env -> TyCon -> ConvertM [Definition]
convertVariantDef env tycon = do
    (env', tyVars) <- bindTypeVars env (tyConTyVars tycon)
    (constrs, moreDefs) <- mapAndUnzipM
        (convertVariantConDef env' tycon tyVars)
        (tyConDataCons tycon)
    let tconName = mkTypeCon [getOccText tycon]
        typeDef = defDataType tconName tyVars (DataVariant constrs)
    pure $ [typeDef] ++ concat moreDefs

convertVariantConDef :: Env -> TyCon -> [(TypeVarName, LF.Kind)] -> DataCon -> ConvertM ((VariantConName, LF.Type), [Definition])
convertVariantConDef env tycon tyVars con =
    case (ctorLabels con, dataConOrigArgTys con) of
        ([], []) ->
            pure ((ctorName, TUnit), [])
        ([], [argTy]) -> do
            argTy' <- convertType env argTy
            pure ((ctorName, argTy'), [])
        ([], _:_:_) ->
            unsupported "Data constructor with multiple unnamed fields." (prettyPrint (getName tycon))
        (labels, args) -> do
            fields <- zipExact labels <$> mapM (convertType env) args
            let recName = synthesizeVariantRecord ctorName tconName
                recDef = defDataType recName tyVars (DataRecord fields)
                recType = TConApp
                    (qualifyLocally env recName)
                    (map (TVar . fst) tyVars)
            pure ((ctorName, recType), [recDef])
    where
        tconName = mkTypeCon [getOccText tycon]
        ctorName = mkVariantCon (getOccText con)

this, self, arg, res :: ExprVarName
this = mkVar "this"
self = mkVar "self"
arg = mkVar "arg"
res = mkVar "res"

convertTemplateDefs :: Env -> ModuleContents -> ConvertM [Definition]
convertTemplateDefs env mc =
    forM (MS.toList (mcTemplateBinds mc)) $ \(tname, tbinds) -> do
        resetFreshVarCounters
        DTemplate <$> convertTemplate env mc tname tbinds

convertTemplate :: Env -> ModuleContents -> LF.TypeConName -> TemplateBinds -> ConvertM Template
convertTemplate env mc tplTypeCon tbinds@TemplateBinds{..}
    | Just tplTyCon <- tbTyCon
    , Just fSignatory <- tbSignatory
    , Just fObserver <- tbObserver
    , Just fEnsure <- tbEnsure
    , Just fAgreement <- tbAgreement
    , tplLocation <- convNameLoc (GHC.tyConName tplTyCon)
    = withRange tplLocation $ do
        let tplParam = this
        tplSignatories <- useSingleMethodDict env fSignatory (`ETmApp` EVar this)
        tplObservers <- useSingleMethodDict env fObserver (`ETmApp` EVar this)
        tplPrecondition <- useSingleMethodDict env fEnsure (wrapPrecondition . (`ETmApp` EVar this))
        tplAgreement <- useSingleMethodDict env fAgreement (`ETmApp` EVar this)
        tplChoices <- convertChoices env mc tplTypeCon tbinds
        tplKey <- convertTemplateKey env tplTypeCon tbinds
        tplImplements <- convertImplements env mc tplTypeCon
        pure Template {..}

    | otherwise =
        unhandled ("Missing required instances in template definition for " <> show tplTypeCon) ()

  where
    wrapPrecondition b
        | envLfVersion env`supports` featureExceptions
        = case tbShow of
            Nothing ->
                error ("Missing Show instance for template: " <> show tplTypeCon)
            Just showDict ->
                ECase b
                    [ CaseAlternative (CPBool True) ETrue
                    , CaseAlternative (CPBool False)
                        $ EThrow TBool (TCon preconditionFailedTypeCon)
                        $ mkPreconditionFailed
                        $ EBuiltin BEAppendText
                            `ETmApp` EBuiltin (BEText "Template precondition violated: " )
                            `ETmApp`
                                (EStructProj (FieldName "m_show")
                                    (EVal (qualifyLocally env (convVal showDict)))
                                `ETmApp` EUnit
                                `ETmApp` EVar this)
                    ]

        | otherwise
        = b


convertTemplateKey :: Env -> LF.TypeConName -> TemplateBinds -> ConvertM (Maybe TemplateKey)
convertTemplateKey env tname TemplateBinds{..}
    | Just keyTy <- tbKeyType
    , Just fKey <- tbKey
    , Just fMaintainer <- tbMaintainer
    = do
        let qtname = qualifyLocally env tname
        tplKeyType <- convertType env keyTy
        tplKeyBody <- useSingleMethodDict env fKey (`ETmApp` EVar this)
        tplKeyMaintainers <- useSingleMethodDict env fMaintainer
            (\f -> f `ETyApp` TBuiltin BTList `ETmApp` ENil (TCon qtname))
        pure $ Just TemplateKey {..}

    | otherwise
    = pure Nothing

convertExceptionDefs :: Env -> ModuleContents -> ConvertM [Definition]
convertExceptionDefs env mc =
    forM (MS.toList (mcExceptionBinds mc)) $ \(ename, ebinds) -> do
        resetFreshVarCounters
        DException <$> convertDefException env ename ebinds

convertDefException :: Env -> LF.TypeConName -> ExceptionBinds -> ConvertM DefException
convertDefException env exnName ExceptionBinds{..} = do
    let exnLocation = convNameLoc (GHC.tyConName ebTyCon)
    withRange exnLocation $ do
        exnMessage <- useSingleMethodDict env ebMessage id
        pure DefException {..}

-- | Convert the method from a single method type class dictionary
-- (such as those used in template desugaring), and then fmap over it
-- (usually to apply some arguments).
useSingleMethodDict :: Env -> GHC.Expr Var -> (LF.Expr -> t) -> ConvertM t
useSingleMethodDict env (Cast ghcExpr _) f = do
    lfExpr <- convertExpr env ghcExpr
    pure (f lfExpr)
useSingleMethodDict env x _ =
    unhandled "useSingleMethodDict: not a single method type class dictionary" x

convertImplements :: Env -> ModuleContents -> LF.TypeConName -> ConvertM (NM.NameMap TemplateImplements)
convertImplements env mc tpl = NM.fromList <$>
  mapM convertImplements1
    (maybe [] interfaceInstanceGroupBinds (MS.lookup tpl (mcInterfaceInstanceBinds mc)))
  where
    convertImplements1 :: InterfaceInstanceBinds -> ConvertM TemplateImplements
    convertImplements1 =
      convertInterfaceInstance
        (TemplateOrInterface.Template tpl)
        (\iface _ -> TemplateImplements iface)
        env

convertInterfaceInstance ::
     TemplateOrInterface' TypeConName
  -> (Qualified TypeConName -> Qualified TypeConName -> InterfaceInstanceBody -> r)
  -> Env
  -> InterfaceInstanceBinds
  -> ConvertM r
convertInterfaceInstance parent mkR env iib = withRange (iibLoc iib) do
  unless (envLfVersion env `supports` featureSimpleInterfaces) do
    unsupported "Daml interfaces are only available with --target=1.15 or higher" ()
  interfaceQualTypeCon <- qualifyInterfaceCon (iibInterface iib)
  templateQualTypeCon <- qualifyTemplateCon (iibTemplate iib)
  checkParent interfaceQualTypeCon templateQualTypeCon
  methods <- convertMethods (iibMethods iib)
  view <- convertView (iibView iib)
  pure $ mkR
    interfaceQualTypeCon
    templateQualTypeCon
    (InterfaceInstanceBody methods view)
  where
    qualifyInterfaceCon =
      convertInterfaceTyCon env handleIsNotInterface
      where
        handleIsNotInterface tyCon =
          mkErr $ "'" <> prettyPrint tyCon <> "' is not an interface"

    qualifyTemplateCon =
      convertTemplateTyCon env handleIsNotTemplate
      where
        handleIsNotTemplate tyCon =
          mkErr $ "'" <> prettyPrint tyCon <> "' is not a template"

    checkParent interfaceQualTypeCon templateQualTypeCon =
      case parent of
        TemplateOrInterface.Template t ->
          checkParent' "template" (qualifyLocally env t == templateQualTypeCon)
        TemplateOrInterface.Interface i ->
          checkParent' "interface" (qualifyLocally env i == interfaceQualTypeCon)
      where
        checkParent' tOrI check = do
          unless check $ conversionError $ mkErr $ unwords
            [ "The", tOrI, "of this interface instance does not match the"
            , "enclosing", tOrI, "declaration."
            ]

    convertMethods ms = fmap NM.fromList . sequence $
      [ InterfaceInstanceMethod k . (`ETmApp` EVar this) <$> convertExpr env v
      | (k, v) <- MS.assocs ms
      ]

    convertView vs = case vs of
        [view] -> do
            viewLFExpr <- convertExpr env view
            pure $ viewLFExpr `ETmApp` EVar this
        [] -> conversionError $ mkErr "no view implementation defined"
        _ -> conversionError $ mkErr "more than one view implementation defined"

    mkErr s = unwords
        [ "Invalid 'interface instance"
        , prettyPrint (iibInterface iib)
        , "for"
        , prettyPrint (iibTemplate iib) <> "':"
        , s
        ]

convertChoices :: Env -> ModuleContents -> LF.TypeConName -> TemplateBinds -> ConvertM (NM.NameMap TemplateChoice)
convertChoices env mc tplTypeCon tbinds =
    NM.fromList <$> traverse (convertChoice env tbinds)
        (MS.findWithDefault [] tplTypeCon (mcChoiceData mc))

convertChoice :: Env -> TemplateBinds -> ChoiceData -> ConvertM TemplateChoice
convertChoice env tbinds (ChoiceData ty expr) = do
    TConApp _ [_, _ :-> _ :-> choiceTy@(TConApp choiceTyCon _) :-> TUpdate choiceRetTy, consumingTy, _] <- convertType env ty
    let choiceName = ChoiceName (T.intercalate "." $ unTypeConName $ qualObject choiceTyCon)
    ERecCon _ [ (_, controllers)
              , (_, action)
              , _
              , (_, optObservers)
              ] <- removeLocations <$> convertExpr env expr

    mbObservers <-
      case optObservers of
        ENone{} -> pure Nothing
        ESome{someBody} -> pure $ Just someBody
        _ -> unhandled "choice observers function" optObservers

    consuming <- convertConsuming consumingTy
    let update = action `ETmApp` EVar self `ETmApp` EVar this `ETmApp` EVar arg
    update <- case consuming of
        Consuming -> pure update
        NonConsuming -> pure update
        PreConsuming | Just fArchive <- tbArchive tbinds -> do
          archiveSelf <- useSingleMethodDict env fArchive (`ETmApp` EVar self)
          pure $ EUpdate $ UBind (Binding (mkVar "_", TUnit) archiveSelf) update
        PreConsuming | otherwise ->
          unsupported "Preconsuming choice for interface." ()
        PostConsuming | Just fArchive <- tbArchive tbinds -> do
          archiveSelf <- useSingleMethodDict env fArchive (`ETmApp` EVar self)
          pure $
            EUpdate $ UBind (Binding (res, choiceRetTy) update) $
            EUpdate $ UBind (Binding (mkVar "_", TUnit) archiveSelf) $
            EUpdate $ UPure choiceRetTy $ EVar res
        PostConsuming | otherwise ->
          unsupported "Postconsuming choice for interface." ()
    pure TemplateChoice
        { chcLocation = Nothing
        , chcName = choiceName
        , chcConsuming = consuming == Consuming
        , chcControllers = applyThisAndArg controllers
        , chcObservers = applyThisAndArg <$> mbObservers
        , chcSelfBinder = self
        , chcArgBinder = (arg, choiceTy)
        , chcReturnType = choiceRetTy
        , chcUpdate = update
        }
      where
        applyThisAndArg func = func `ETmApp` EVar this `ETmApp` EVar arg

convertBinds :: Env -> ModuleContents -> ConvertM [Definition]
convertBinds env mc =
  concatMapM (\bind -> resetFreshVarCounters >> convertBind env mc bind) (mcBinds mc)

convertBind :: Env -> ModuleContents -> (Var, GHC.Expr Var) -> ConvertM [Definition]
convertBind env mc (name, x)
    -- This is inlined in the choice in the template so we can just drop this.
    | "_choice_" `T.isPrefixOf` getOccText name
    = pure []
    -- We only need this to get additional info for interface choices.
    | "_interface_choice_" `T.isPrefixOf` getOccText name
    = pure []
    -- These are only used as markers for the LF conversion.
    | "_interface_instance_" `T.isPrefixOf` getOccText name
    = pure []
    | "_requires_" `T.isPrefixOf` getOccText name
    = pure []
    -- These are moved into interface implementations so we can drop them
    | "_method_" `T.isPrefixOf` getOccText name
    = pure []
    | "_view_" `T.isPrefixOf` getOccText name
    = pure []

    -- Remove guarded exercise when Extended Interfaces are unsupported
    | not (envLfVersion env `supports` featureExtendedInterfaces)
    , "$cexerciseGuarded" `T.isPrefixOf` getOccText name
    = pure []

    -- Remove guarded exercise when Extended Interfaces are unsupported
    | not (envLfVersion env `supports` featureExtendedInterfaces)
    , NameIn DA_Internal_Template_Functions "exerciseGuarded" <- name
    = pure []

    | not (envLfVersion env `supports` featureExtendedInterfaces)
    , DesugarDFunId _ _ (NameIn DA_Internal_Template_Functions "HasExerciseGuarded") _ <- name
    = pure []

    -- Remove internal functions.
    | Just internals <- lookupUFM internalFunctions (envGHCModuleName env)
    , getOccFS name `elementOfUniqSet` internals
    = pure []

    -- Remove interface worker.
    | Just iface <- T.stripPrefix "$W" (getOccText name)
    , mkTypeCon [iface] `MS.member` mcInterfaceBinds mc = pure []

    -- NOTE(MH): Our inline return type syntax produces a local letrec for
    -- recursive functions. We currently don't support local letrecs.
    -- However, we can work around this issue by rewriting
    -- > name = \@a_1 ... @a_n -> letrec f = \v -> y in f  -- (n >= 0)
    -- into
    -- > name = \@a_1 ... @a_n -> \v -> let f = name @a_1 ... @a_n in y
    -- We need to make sure `f` is a term lambda here since we'd end up with a
    -- recursive value otherwise.
    -- (This rewriting can be regarded as a very limited form of lambda
    -- lifting where the lifted version of `f` happens to be `name`.)
    -- This workaround should be removed once we either have a proper lambda
    -- lifter or Daml-LF supports local recursion.
    --
    -- NOTE(SF): Due to issue #7953, this has been modified to allow for
    -- additional (nonrecursive) let bindings between the top-level
    -- arguments and the letrec. In particular,
    --
    --  > name = \@a_1 ... @a_n ->
    --  >    let { x_1 = e_1 ; ... ; x_m = e_m } in
    --  >    letrec f = \v -> y in f
    --
    -- is rewritten to
    --
    --  > name = \@a_1 ... @a_n ->
    --  >    let { x_1 = e_1 ; ... ; x_m = e_m } in
    --  >    \v -> let f = name @a_1 ... @a_n in y
    --
    | let (params, body1) = collectBinders x
    , let (lets, body2) = collectNonRecLets body1
    , Let (Rec [(f, Lam v y)]) (Var f') <- body2
    , f == f'
    = convertBind env mc $ (,) name $ mkLams params $ makeNonRecLets lets $
        Lam v $ Let (NonRec f $ mkVarApps (Var name) params) y

    -- Constraint tuple projections are turned into LF struct projections at use site.
    | ConstraintTupleProjectionName _ _ <- name
    = pure []

    -- HasMethod instances are only used for desugaring.
    -- In data-dependencies, they are reconstructed from the interface definition.
    | DFunId _ <- idDetails name
    , TypeCon hasMethodCls _ <- varType name
    , NameIn DA_Internal_Desugar "HasMethod" <- hasMethodCls
    = pure []

    -- Typeclass instance dictionaries
    | DFunId isNewtype <- idDetails name
    = withRange (convNameLoc name) $ do
    x' <- convertExpr env x
    -- NOTE(MH): This is DICTIONARY SANITIZATION step (3).
    -- The sanitization for newtype dictionaries is done in `convertCoercion`.
    let sanitized_x'
          | isNewtype = x'
          | otherwise =
            over (_ETyLams . _2 . _ETmLams . _2 . _EStructCon . each . _2) (ETmLam (mkVar "_", TUnit)) x'
    name' <- convValWithType env name

    -- OVERLAP* annotations
    let overlapModeName' = overlapModeName (fst name')
        overlapModeDef = maybeToList $ do
            overlapMode <- MS.lookup name (mcModInstanceInfo mc)
            overlapModeType <- encodeOverlapMode overlapMode
            Just (DValue (mkMetadataStub overlapModeName' overlapModeType))

    pure $ [defValue name name' sanitized_x'] ++ overlapModeDef

    -- Scenario definitions when scenarios are disabled
    | EnableScenarios False <- envEnableScenarios env
    , ty@(TypeCon scenarioType [_]) <- varType name -- Scenario : * -> *
    , NameIn DA_Internal_LF "Scenario" <- scenarioType
    = withRange (convNameLoc name) $ conversionError $ unlines
        [ "Scenarios are no longer supported."
        , "Instead, consider using Daml Script (https://docs.daml.com/daml-script/index.html)."
        , "When compiling " <> prettyPrint name <> " : " <> prettyPrint ty <> "."
        ]

    -- Regular functions
    | otherwise
    = withRange (convNameLoc name) $ do
    x' <- convertExpr env x
    name' <- convValWithType env name
    pure [defValue name name' x']

-- NOTE(MH): These are the names of the builtin Daml-LF types whose Surface
-- Daml counterpart is not defined in 'GHC.Types'. They are all defined in
-- 'DA.Internal.LF' in terms of 'GHC.Types.Opaque'. We need to remove them
-- during conversion to Daml-LF together with their constructors since we
-- deliberately remove 'GHC.Types.Opaque' as well.
internalTypes :: UniqSet FastString
internalTypes = mkUniqSet
    [ "Scenario", "Update", "ContractId", "Time", "Date", "Party"
    , "Pair", "TextMap", "Map", "Any", "TypeRep"
    , "AnyException"
    , "Experimental"
    ]

desugarTypes :: UniqSet FastString
desugarTypes = mkUniqSet
    [ "Consuming"
    , "PreConsuming"
    , "PostConsuming"
    , "NonConsuming"
    , "Method"
    , "HasMethod"
    , "InterfaceInstance"
    , "RequiresT"
    , "InterfaceView"
    ]

internalFunctions :: UniqFM (UniqSet FastString)
internalFunctions = listToUFM $ map (bimap mkModuleNameFS mkUniqSet)
    [ ("DA.Internal.Record",
        [ "getFieldPrim"
        , "setFieldPrim"
        ])
    , ("DA.Internal.LF", "unpackPair" : map ("$W" <>) (nonDetEltsUniqSet internalTypes))
    , ("GHC.Base",
        [ "getTag"
        ])
    , ("DA.Internal.Desugar",
        [ "mkInterfaceInstance"
        , "mkMethod"
        , "mkInterfaceView"
        , "codeGenAllowLargeTuples"
        ])
    ]

convertExpr :: Env -> GHC.Expr Var -> ConvertM LF.Expr
convertExpr env0 e = do
    (e, args) <- go env0 e []
    let appArg e (mbSrcSpan, arg) =
            mkEApp (maybe id (ELocation . convRealSrcSpan) mbSrcSpan e) <$> convertArg env0 arg
    foldlM appArg e args
  where
    -- NOTE(MH): We collect all arguments to functions in a list such that
    -- we have access to them when converting functions. This is necessary to
    -- inline fully applied record constructors instead of going through a
    -- record constructor function, which is required for contract keys.
    go :: Env -> GHC.Expr Var -> [LArg Var] -> ConvertM (LF.Expr, [LArg Var])
    go env (Tick (SourceNote srcSpan _) x) args = case args of
        [] -> do
            x <- convertExpr env x
            pure (ELocation (convRealSrcSpan srcSpan) x, [])
        (_, arg):args -> go env x ((Just srcSpan, arg):args)
    go env (Tick _ x) args = go env x args
    go env (x `App` y) args
        = go env x ((Nothing, y) : args)
    -- see through $ to give better matching power
    go env (VarIn GHC_Base "$") (LType _ : LType _ : LType _ : LExpr x : y : args)
        = go env x (y : args)
    go env (VarIn DA_Internal_LF "unpackPair") (LType (StrLitTy f1) : LType (StrLitTy f2) : LType t1 : LType t2 : args)
        = fmap (, args) $ do
            t1 <- convertType env t1
            t2 <- convertType env t2
            let fields = [(mkField f1, t1), (mkField f2, t2)]
            tupleTyCon <- qDA_Types env $ mkTypeCon ["Tuple" <> T.pack (show $ length fields)]
            let tupleType = TypeConApp tupleTyCon (map snd fields)
            v <- freshTmVar
            let mkFieldProj i (name, _typ) = (mkIndexedField i, EStructProj name (EVar v))
            pure $ ETmLam (v, TStruct fields) $ ERecCon tupleType $ zipWithFrom mkFieldProj (1 :: Int) fields
    go env (VarIn GHC_Types "primitive") (LType (isStrLitTy -> Just y) : LType t : args)
        = fmap (, args) $ convertPrim (envLfVersion env) (unpackFS y) =<< convertType env t
    -- erase mkMethod calls and leave only the body.
    go env (VarIn DA_Internal_Desugar "mkMethod") (LType _parent : LType _iface : LType _tpl : LType _methodName : LType _methodTy : LExpr _implDict : LExpr _hasMethodDic : LExpr body : args)
        = go env body args
    -- erase mkInterfaceView calls and leave only the body.
    go env (VarIn DA_Internal_Desugar "mkInterfaceView") (LType _parent : LType _iface : LType _tpl : LType _viewTy : LExpr _implDict : LExpr _hasInterfaceViewDic : LExpr body : args)
        = go env body args
    go env (VarIn GHC_Types "primitiveInterface") (LType (isStrLitTy -> Just y) : LType t : args)
        = do
        ty <- convertType env t
        case ty of
            TCon iface :-> _ ->
                pure
                  ( ETmLam (mkVar "i", TCon iface) $
                    ECallInterface iface (MethodName $ T.pack $ unpackFS y) (EVar $ mkVar "i")
                  , args
                  )
            _ -> unsupported "primitiveInterface not applied to function from interface." t
    -- NOTE(MH): `getFieldPrim` and `setFieldPrim` are used by the record
    -- preprocessor to magically implement the `HasField` instances for records.
    go env (VarIn DA_Internal_Record "getFieldPrim") (LType (isStrLitTy -> Just name) : LType record : LType _field : args) = do
        record' <- convertType env record
        withTmArg env record' args $ \x args ->
            pure (ERecProj (fromTCon record') (mkField $ fsToText name) x, args)
    go env (VarIn DA_Internal_Record "setFieldPrim") (LType (isStrLitTy -> Just name) : LType record : LType field : args) = do
        record' <- convertType env record
        field' <- convertType env field
        withTmArg env field' args $ \x1 args ->
            withTmArg env record' args $ \x2 args ->
                pure (ERecUpd (fromTCon record') (mkField $ fsToText name) x2 x1, args)
    -- NOTE(MH): We only inline `getField` for record types. Projections on
    -- sum-of-records types have to through the type class for `getField`.
    go env (VarIn DA_Internal_Record "getField") (LType (isStrLitTy -> Just name) : LType recordType@(TypeCon recordTyCon _) : LType _fieldType : _dict : args)
        | isSingleConType recordTyCon = do
            recordType <- convertType env recordType
            withTmArg env recordType args $ \record args ->
                pure (ERecProj (fromTCon recordType) (mkField $ fsToText name) record, args)
    -- NOTE(SF): We also need to inline `setField` in order to get the correct
    -- evaluation order (record first, then fields in order).
    go env (VarIn DA_Internal_Record "setField") (LType (isStrLitTy -> Just name) : LType record@(TypeCon recordTyCon _) : LType field : _dict : args)
        | isSingleConType recordTyCon = do
            record' <- convertType env record
            field' <- convertType env field
            withTmArg env field' args $ \x1 args ->
                withTmArg env record' args $ \x2 args ->
                    pure (ERecUpd (fromTCon record') (mkField $ fsToText name) x2 x1, args)
        -- TODO: Also fix evaluation order for sum-of-record types.
    go env (VarIn GHC_Real "fromRational") (LExpr (VarIs ":%" `App` tyInteger `App` Lit (LitNumber _ top _) `App` Lit (LitNumber _ bot _)) : args)
        = fmap (, args) $ convertRationalDecimal env top bot
    go env (VarIn GHC_Real "fromRational") (LType (isNumLitTy -> Just n) : _ : LExpr (VarIs ":%" `App` tyInteger `App` Lit (LitNumber _ top _) `App` Lit (LitNumber _ bot _)) : args)
        = fmap (, args) $ convertRationalNumericMono env n top bot
    go env (VarIn GHC_Real "fromRational") (LType (TypeCon (NameIn GHC_Types "Numeric") [isNumLitTy -> Just n]) : _ : LExpr (VarIs ":%" `App` tyInteger `App` Lit (LitNumber _ top _) `App` Lit (LitNumber _ bot _)) : args)
        = fmap (, args) $ convertRationalNumericMono env n top bot
    go env (VarIn GHC_Real "fromRational") (LType (TypeCon (NameIn GHC_Types "BigNumeric") []) : _ : LExpr (VarIs ":%" `App` tyInteger `App` Lit (LitNumber _ top _) `App` Lit (LitNumber _ bot _)) : args)
        = fmap (, args) $ convertRationalBigNumeric top bot
    go env (VarIn GHC_Real "fromRational") (LType scaleTyCoRep : _ : LExpr (VarIs ":%" `App` tyInteger `App` Lit (LitNumber _ top _) `App` Lit (LitNumber _ bot _)) : args)
        = unsupported ("Polymorphic numeric literal. Specify a fixed scale by giving the type, e.g. (" ++ show (fromRational (top % bot) :: Decimal.Decimal) ++ " : Numeric 10).") ()
    go env (VarIn GHC_Num "negate") (tyInt : LExpr (VarIs "$fAdditiveInt") : LExpr (untick -> VarIs "fromInteger" `App` Lit (LitNumber _ x _)) : args)
        = fmap (, args) $ convertInt64 (negate x)
    go env (VarIn GHC_Integer_Type "fromInteger") (LExpr (Lit (LitNumber _ x _)) : args)
        = fmap (, args) $ convertInt64 x
    go env (Lit (LitNumber LitNumInt x _)) args
        = fmap (, args) $ convertInt64 x
    go env (VarIn Data_String "fromString") (LExpr x : args) -- referenced by deriving Enum
        = fmap (, args) $ convertExpr env x
    go env (VarIn GHC_CString "fromString") (LExpr x : args)
        = fmap (, args) $ convertExpr env x
    go env (VarIn GHC_CString "unpackCString#") (LExpr (Lit (LitString x)) : args)
        = pure $ (, args) $ EBuiltin $ BEText $ unpackCString x
    go env (VarIn GHC_CString "unpackCStringUtf8#") (LExpr (Lit (LitString x)) : args)
        = pure $ (, args) $ EBuiltin $ BEText $ unpackCStringUtf8 x
    go env x@(VarIn Control_Exception_Base _) (LType t1 : LType t2 : LExpr (untick -> Lit (LitString s)) : args)
        = fmap (, args) $ do
        x' <- convertExpr env x
        t1' <- convertType env t1
        t2' <- convertType env t2
        pure (x' `ETyApp` t1' `ETyApp` t2' `ETmApp` EBuiltin (BEText (unpackCStringUtf8 s)))
    go env (VarIn DA_Internal_Template_Functions "exerciseGuarded") _
        | not $ envLfVersion env `supports` featureExtendedInterfaces
        = conversionError "Guarded exercises are only available with --target=1.dev"

    go env (ConstraintTupleProjection index arity) args
        | (LExpr x : args') <- drop arity args -- drop the type arguments
        = fmap (, args') $ do
            let fieldName = mkSuperClassField index
            x' <- convertExpr env x
            pure $ EStructProj fieldName x'

    -- conversion of bodies of $con2tag functions
    go env (VarIn GHC_Base "getTag") (LType (TypeCon t _) : LExpr x : args) = fmap (, args) $ do
        x' <- convertExpr env x
        t' <- convertQualifiedTyCon env t
        let mkCasePattern con
                -- Note that tagToEnum# can also be used on non-enum types, i.e.,
                -- types where not all constructors are nullary.
                | isEnumTyCon t = CPEnum t' con
                | otherwise = CPVariant t' con (mkVar "_")
        pure $ ECase x'
            [ CaseAlternative
                (mkCasePattern (mkVariantCon (getOccText con)))
                (EBuiltin $ BEInt64 i)
            | (con, i) <- zip (tyConDataCons t) [0..]
            ]
    go env (VarIn GHC_Prim "tagToEnum#") (LType (TypeCon (Is "Bool") []) : LExpr (op0 `App` x `App` y) : args)
        | VarIn GHC_Prim "==#" <- op0 = go mkBuiltinEqual
        | VarIn GHC_Prim "<#"  <- op0 = go mkBuiltinLess
        | VarIn GHC_Prim ">#"  <- op0 = go mkBuiltinGreater
        where
          go op1 = fmap (, args) $ do
              x' <- convertExpr env x
              y' <- convertExpr env y
              pure (op1 BTInt64 `ETmApp` x' `ETmApp` y')
    go env (VarIn GHC_Prim "tagToEnum#") (LType (TypeCon (Is "Bool") []) : LExpr x : args) = fmap (, args) $ do
        x' <- convertExpr env x
        pure $ mkBuiltinEqual BTInt64 `ETmApp` EBuiltin (BEInt64 1) `ETmApp` x'
    go env (VarIn GHC_Prim "tagToEnum#") (LType tt@(TypeCon t _) : LExpr x : args) = fmap (, args) $ do
        -- FIXME: Should generate a binary tree of eq and compare
        tt' <- convertType env tt
        x' <- convertExpr env x
        let cs = tyConDataCons t
            c1 = head cs -- FIXME: handle the empty variant more gracefully.
            mkCtor con
              -- Note that tagToEnum# can also be used on non-enum types, i.e.,
              -- types where not all constructors are nullary.
              | isEnumTyCon t
              = EEnumCon (tcaTypeCon (fromTCon tt')) (mkVariantCon (getOccText con))
              | otherwise
              = EVariantCon (fromTCon tt') (mkVariantCon (getOccText con)) EUnit
            mkEqInt i = mkBuiltinEqual BTInt64 `ETmApp` x' `ETmApp` EBuiltin (BEInt64 i)
        pure (foldr ($) (mkCtor c1) [mkIf (mkEqInt i) (mkCtor c) | (i,c) <- zipFrom 0 cs])

    -- built ins because they are lazy
    go env (VarIn GHC_Types "ifThenElse") (LType tRes : LExpr cond : LExpr true : LExpr false : args)
        = fmap (, args) $ mkIf <$> convertExpr env cond <*> convertExpr env true <*> convertExpr env false
    go env (VarIn GHC_Classes "||") (LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> pure ETrue <*> convertExpr env y
    go env (VarIn GHC_Classes "&&") (LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> convertExpr env y <*> pure EFalse
    go env (VarIn DA_Action "when") (LType monad : LExpr dict : LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> convertExpr env y <*> mkPure env monad dict TUnit EUnit
    go env (VarIn DA_Action "unless") (LType monad : LExpr dict : LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> mkPure env monad dict TUnit EUnit <*> convertExpr env y
    go env submit@(VarIn DA_Internal_LF "submit") (LType m : LType cmds : LExpr dict : LType typ : LExpr callstack : LExpr pty : LExpr upd : args) = fmap (, args) $ do
         m' <- convertType env m
         typ' <- convertType env typ
         pty' <- convertExpr env pty
         upd' <- convertExpr env upd
         case m' of
           TBuiltin BTScenario -> pure $ EScenario (SCommit typ' pty' (EUpdate (UEmbedExpr typ' upd')))
           _ -> do
             submit' <- convertExpr env submit
             cmds' <- convertType env cmds
             dict' <- convertExpr env dict
             callstack' <- convertExpr env callstack
             pure $ mkEApps submit' [TyArg m', TyArg cmds', TmArg dict', TyArg typ', TmArg callstack', TmArg pty', TmArg upd']
    go env submitMustFail@(VarIn DA_Internal_LF "submitMustFail") (LType m : LType cmds : LExpr dict : LType typ : LExpr callstack : LExpr pty : LExpr upd : args) = fmap (, args) $ do
         m' <- convertType env m
         typ' <- convertType env typ
         pty' <- convertExpr env pty
         upd' <- convertExpr env upd
         case m' of
           TBuiltin BTScenario -> pure $ EScenario (SMustFailAt typ' pty' (EUpdate (UEmbedExpr typ' upd')))
           _ -> do
             submitMustFail' <- convertExpr env submitMustFail
             cmds' <- convertType env cmds
             dict' <- convertExpr env dict
             callstack' <- convertExpr env callstack
             pure $ mkEApps submitMustFail' [TyArg m', TyArg cmds', TmArg dict', TyArg typ', TmArg callstack', TmArg pty', TmArg upd']

    -- custom conversion because they correspond to builtins in Daml-LF, so can make the output more readable
    go env (VarIn DA_Internal_Prelude "pure") (LType monad : LExpr dict : LType t : LExpr x : args)
        -- This is generating the special UPure/SPure nodes when the monad is Update/Scenario.
        = fmap (, args) $ join $ mkPure env monad dict <$> convertType env t <*> convertExpr env x
    go env (VarIn DA_Internal_Prelude "return") (LType monad : LType t : LExpr dict : LExpr x : args)
        -- This is generating the special UPure/SPure nodes when the monad is Update/Scenario.
        -- In all other cases, 'return' is rewritten to 'pure'.
        = fmap (, args) $ join $ mkPure env monad dict <$> convertType env t <*> convertExpr env x
    go env bind@(VarIn DA_Internal_Prelude ">>=") allArgs@(LType monad : LExpr dict : LType _ : LType _ : LExpr x : LExpr lam@(Lam b y) : args) = do
        monad' <- convertType env monad
        case monad' of
          TBuiltin BTUpdate -> mkBind EUpdate UBind
          TBuiltin BTScenario -> mkBind EScenario SBind
          _ -> fmap (, allArgs) $ convertExpr env bind
        where
          mkBind :: (m -> LF.Expr) -> (Binding -> LF.Expr -> m) -> ConvertM (LF.Expr, [LArg Var])
          mkBind inj bind = fmap (, args) $ do
              x' <- convertExpr env x
              y' <- convertExpr env y
              b' <- convVarWithType env b
              pure (inj (bind (Binding b' x') y'))
    go env semi@(VarIn DA_Internal_Prelude ">>") (LType monad : LType t1 : LType t2 : LExpr dict : LExpr x : LExpr y : args) = fmap (, args) $ do
        monad' <- convertType env monad
        dict' <- convertExpr env dict
        t1' <- convertType env t1
        t2' <- convertType env t2
        x' <- convertExpr env x
        y' <- convertExpr env y
        case monad' of
          TBuiltin BTUpdate -> pure $ EUpdate (UBind (Binding (mkVar "_", t1') x') y')
          TBuiltin BTScenario -> pure $ EScenario (SBind (Binding (mkVar "_", t1') x') y')
          _ -> do
            EVal semi' <- convertExpr env semi
            let bind' = EVal semi'{qualObject = mkVal ">>="}
            pure $ mkEApps bind' [TyArg monad', TmArg dict', TyArg t1', TyArg t2', TmArg x', TmArg (ETmLam (mkVar "_", t1') y')]
    go env tryCatch@(VarIn DA_Internal_Exception "_tryCatch") allArgs@(LType monad : LExpr _dict : LType t : LExpr x : LExpr (Lam b y) : args) = do
        monad' <- convertType env monad
        case monad' of
            TBuiltin BTUpdate -> do
                t' <- convertType env t
                x' <- convertExpr env x
                y' <- convertExpr env y
                pure (EUpdate (UTryCatch t' (ETmApp x' EUnit) (convVar b) y'), args)
            _ ->
                fmap (, allArgs) $ convertExpr env tryCatch

    go env (VarIn GHC_Types "[]") (LType (TypeCon (Is "Char") []) : args)
        = pure $ (, args) $ EBuiltin (BEText T.empty)
    go env (VarIn GHC_Types "[]") args
        = withTyArg env KStar args $ \env t args -> pure (ENil t, args)
    go env (VarIn GHC_Types ":") args =
        withTyArg env KStar args $ \env t args ->
        withTmArg env t args $ \x args ->
        withTmArg env (TList t) args $ \y args ->
          pure (ECons t x y, args)
    go env (VarIn DA_Internal_Prelude (IgnoreWorkerPrefixFS "None")) args
        = withTyArg env KStar args $ \env t args -> pure (ENone t, args)
    go env (VarIn DA_Internal_Prelude (IgnoreWorkerPrefixFS "Some")) args
        = withTyArg env KStar args $ \env t args ->
          withTmArg env t args $ \x args ->
            pure (ESome t x, args)

    go env (VarIn GHC_Tuple "()") args = pure (EUnit, args)

    go env (VarIn GHC_Types (RoundingModeName roundingModeLit)) args =
        pure (EBuiltin (BERoundingMode roundingModeLit), args)

    go env (VarIn GHC_Types "True") args = pure (mkBool True, args)
    go env (VarIn GHC_Types "False") args = pure (mkBool False, args)
    go env (VarIn GHC_Types "I#") args = pure (mkIdentity TInt64, args)
        -- we pretend Int and Int# are the same thing

    go env (Var x) args
        | Just internals <- lookupUFM internalFunctions modName
        , getOccFS x `elementOfUniqSet` internals
        = unsupported "Direct call to internal function." x
        where
            modName = maybe (envGHCModuleName env) GHC.moduleName $ nameModule_maybe $ getName x

    go env (VarIn DA_Internal_Desugar "codeGenAllowLargeTuples") (LType _ : LExpr head : rest)
        = let env' = env { envAllowLargeTuples = AllowLargeTuples True }
           in go env' head rest

    go env (Var x) args
        | Just m <- nameModule_maybe $ varName x
        , Just con <- isDataConId_maybe x
        = convertDataCon env m con args
        | Just m <- nameModule_maybe $ varName x =
            fmap ((, args) . EVal) $ qualify env m $ convVal x
        | isGlobalId x = fmap (, args) $ do
            pkgRef <- nameToPkgRef env $ varName x
            pure $ EVal $ Qualified pkgRef (envLFModuleName env) $ convVal x
            -- some things are global, but not with a module name, so give them the current one
        | Just y <- envLookupAlias x env = pure (y, args)
        | otherwise = pure $ (, args) $ EVar $ convVar x

    go env (Lam name x) args
        | isTyVar name = fmap (, args) $ do
            (env', name') <- bindTypeVar env name
            ETyLam name' <$> convertExpr env' x
        | otherwise = fmap (, args) $
            ETmLam
                <$> convVarWithType env name
                <*> convertExpr env x
    go env (Cast x co) args = fmap (, args) $ do
        x' <- convertExpr env x
        (to, _from) <- convertCoercion env co
        pure $ to x'
    go env (Let (NonRec name x) y) args =
        fmap (, args) $ convertLet env name x (\env -> convertExpr env y)
    go env (Case scrutinee bind _ [(DEFAULT, [], x)]) args =
        go env (Let (NonRec bind scrutinee) x) args
    go env (Case scrutinee bind t [(DataAlt (NameIn GHC_Types "I#"), [v], x)]) args = do
        -- We pretend Int and Int# are the same, so a case-expression becomes a let-expression.
        let letExpr = Let (NonRec bind scrutinee) $ Let (NonRec v (Var bind)) x
        go env letExpr args
    -- NOTE(MH): This is DICTIONARY SANITIZATION step (2).
    go env (Case scrutinee bind _ [(DataAlt con, vs, Var x)]) args
        | tyConFlavour (dataConTyCon con) == ClassFlavour
        = fmap (, args) $ do
            scrutinee' <- convertExpr env scrutinee
            let fldNames = ctorLabels con
            let fldIndex = fromJust (elemIndex x vs)
            let fldName = fldNames !! fldIndex
            recTyp <- convertType env (varType bind)
            pure $ mkDictProj fldName scrutinee' `ETmApp` EUnit
    go env o@(Case scrutinee bind resultType [alt@(DataAlt con, vs, x)]) args = fmap (, args) $ do
        convertType env (varType bind) >>= \case
            -- opaque types have no patterns that can be matched
            TText -> asLet
            TNumeric _ -> asLet
            TParty -> asLet
            TTimestamp -> asLet
            TDate -> asLet
            TContractId{} -> asLet
            TUpdate{} -> asLet
            TScenario{} -> asLet
            TAny{} -> asLet
            tcon | isSimpleRecordCon con || isClassCon con -> do
                let fields = ctorLabels con
                case zipExactMay vs fields of
                    Nothing -> unsupported "Pattern match with existential type." alt
                    Just vsFields -> convertLet env bind scrutinee $ \env -> do
                        bindRef <- convertExpr env (Var bind)
                        x' <- convertExpr env x
                        mkProjBindings env bindRef (fromTCon tcon) vsFields x'
            _ ->
                convertLet env bind scrutinee $ \env -> do
                    bind' <- convertExpr env (Var bind)
                    ty <- convertType env $ varType bind
                    alt' <- convertAlt env ty alt
                    resultType' <- convertType env resultType
                    pure $ mkCase env ty resultType' bind' [alt']

      where
        asLet = convertLet env bind scrutinee $ \env -> convertExpr env x
    go env (Case scrutinee bind resultType (defaultLast -> alts)) args = fmap (, args) $ do
        scrutinee' <- convertExpr env scrutinee
        bindTy <- convertType env $ varType bind
        alts' <- mapM (convertAlt env bindTy) alts
        bind' <- convVarWithType env bind
        resultType' <- convertType env resultType
        if isDeadOcc (occInfo (idInfo bind)) && all isNormalCaseAlternative alts'
        then pure $ mkCase env bindTy resultType' scrutinee' alts'
        else pure $
            ELet (Binding bind' scrutinee') $
            mkCase env bindTy resultType' (EVar $ convVar bind) alts'
    go env (Let (Rec xs) _) args = unsupported "Local variables defined recursively - recursion can only happen at the top level." $ map fst xs
    go env o@(Coercion _) args = unhandled "Coercion" o
    go _ x args = unhandled "Expression" x

-- | Represents a generalised case pattern for a generalised case alternative.
data GeneralisedCasePattern
    = GCPEquality LF.Expr
        -- ^ Pattern matching via built-in equality.
    | GCPNormal CasePattern
        -- ^ Normal case alternative that is directly supported by LF.
        -- This includes the default case (CPDefault).
    deriving (Eq, Ord)

-- | Generalised case alternative
data GeneralisedCaseAlternative = GCA GeneralisedCasePattern LF.Expr
    deriving (Eq, Ord)

-- | Is this a normal case alternative?
isNormalCaseAlternative :: GeneralisedCaseAlternative -> Bool
isNormalCaseAlternative = \case
    GCA (GCPNormal _) _ -> True
    _ -> False

-- | Represents the body of a generalised case expression.
data GeneralisedCaseBody
    = GCBExpr LF.Expr
        -- ^ Expression representing the generalised case.
    | GCBAlts [CaseAlternative]
        -- ^ Alternatives for a regular case statement.

-- | Make a case expression from GeneralisedCaseAlternatives.
--
-- The scrutinee will be evaluated multiple times unless all the
-- case alternatives are normal case alternatives. So to prevent
-- this, this function should only be used when either the scrutinee
-- is inert (e.g. a bound variable), or all the alternatives are
-- normal case alternatives ('GCANormal').
mkCase :: Env -> LF.Type -> LF.Type -> LF.Expr -> [GeneralisedCaseAlternative] -> LF.Expr
mkCase env scrutineeType resultType scrutinee galts =
    finalize (foldr addCaseAlternative (GCBAlts []) galts)
  where
    finalize :: GeneralisedCaseBody -> LF.Expr
    finalize = \case
        GCBExpr e -> e
        GCBAlts [] ->
            ECase scrutinee
                [ CaseAlternative CPDefault
                $ EBuiltin BEError
                    `ETyApp` resultType
                    `ETmApp` EBuiltin (BEText "Unreachable") ]
                -- GHC only generates empty case alternatives if it is sure the scrutinee will fail.
                -- LF doesn't support empty alternatives, so we turn this into a non-empty alternative.
        GCBAlts alts ->
            ECase scrutinee alts

    addCaseAlternative :: GeneralisedCaseAlternative -> GeneralisedCaseBody -> GeneralisedCaseBody
    addCaseAlternative (GCA (GCPNormal pattern) rhs) (GCBAlts alts) =
        GCBAlts (CaseAlternative pattern rhs : alts)
    addCaseAlternative (GCA (GCPNormal pattern) rhs) (GCBExpr e) =
        GCBAlts [CaseAlternative pattern rhs, CaseAlternative CPDefault e]
    addCaseAlternative (GCA (GCPEquality expr) rhs) elseBranch =
        GCBExpr (mkIf (mkScrutineeEquality expr) rhs (finalize elseBranch))

    mkScrutineeEquality :: LF.Expr -> LF.Expr
    mkScrutineeEquality pattern
        = EBuiltin BEEqualGeneric `ETyApp` scrutineeType `ETmApp` scrutinee `ETmApp` pattern

-- | Is this a constraint tuple?
isConstraintTupleTyCon :: TyCon -> Bool
isConstraintTupleTyCon = (Just ConstraintTuple ==) . tyConTuple_maybe

-- | Is this an enum type?
isEnumTyCon :: TyCon -> Bool
isEnumTyCon tycon =
    isEnumerationTyCon tycon
    && (tyConArity tycon == 0)
    && ((length (tyConDataCons tycon) >= 2)
       || hasDamlEnumCtx tycon)

-- | Is this a simple record type?
isSimpleRecordTyCon :: TyCon -> Bool
isSimpleRecordTyCon tycon =
    maybe False isSimpleRecordCon (tyConSingleDataCon_maybe tycon)

-- | Is this a variant type?
isVariantTyCon :: TyCon -> Bool
isVariantTyCon tycon =
    (tyConFlavour tycon == DataTypeFlavour)
    && not (isEnumTyCon tycon)
    && not (isSimpleRecordTyCon tycon)

conIsSingle :: DataCon -> Bool
conIsSingle = isSingleConType . dataConTyCon

conHasNoArgs :: DataCon -> Bool
conHasNoArgs = null . dataConOrigArgTys

conHasLabels :: DataCon -> Bool
conHasLabels = notNull . ctorLabels

isEnumCon :: DataCon -> Bool
isEnumCon = isEnumTyCon . dataConTyCon

isConstraintTupleCon :: DataCon -> Bool
isConstraintTupleCon = isConstraintTupleTyCon . dataConTyCon

isClassCon :: DataCon -> Bool
isClassCon = isClassTyCon . dataConTyCon

isSimpleRecordCon :: DataCon -> Bool
isSimpleRecordCon con =
    (conHasLabels con || conHasNoArgs con)
    && conIsSingle con
    && not (isEnumCon con)
    && not (isConstraintTupleCon con)
    && not (isClassCon con)

isVariantRecordCon :: DataCon -> Bool
isVariantRecordCon con = conHasLabels con && not (conIsSingle con)

-- | The different classes of data cons with respect to LF conversion.
data DataConClass
    = EnumCon -- ^ constructor for an enum type
    | SimpleRecordCon -- ^ constructor for a record type
    | ClassCon -- ^ constructor for a type class
    | SimpleVariantCon -- ^ constructor for a variant type with no synthetic record type
    | VariantRecordCon -- ^ constructor for a variant type with a synthetic record type
    | ConstraintTupleCon -- ^ constructor for a constraint tuple
    deriving (Eq, Show)

classifyDataCon :: DataCon -> DataConClass
classifyDataCon con
    | isEnumCon con = EnumCon
    | isConstraintTupleCon con = ConstraintTupleCon
    | isClassCon con = ClassCon
    | isSimpleRecordCon con = SimpleRecordCon
    | isVariantRecordCon con = VariantRecordCon
    | otherwise = SimpleVariantCon
        -- in which case, daml-preprocessor ensures that the
        -- constructor cannot have more than one argument

-- | Split args into type args and non-type args of the expected length
-- for a particular DataCon.
splitConArgs_maybe :: DataCon -> [LArg Var] -> Maybe ([GHC.Type], [GHC.Arg Var])
splitConArgs_maybe con args = do
    let (conTypes, conTheta, conArgs, _) = dataConSig con
        numTypes = length conTypes
        numVals = length conTheta + length conArgs
        (typeArgs, valArgs) = splitAt numTypes (map snd args)
    guard (length typeArgs == numTypes)
    guard (length valArgs == numVals)
    typeArgs <- mapM isType_maybe typeArgs
    guard (all (isNothing . isType_maybe) valArgs)
    Just (typeArgs, valArgs)

-- NOTE(MH): Handle data constructors. Fully applied record
-- constructors are inlined. This is required for contract keys to
-- work. Constructor workers are not handled (yet).
convertDataCon :: Env -> GHC.Module -> DataCon -> [LArg Var] -> ConvertM (LF.Expr, [LArg Var])
convertDataCon env m con args
    | AllowLargeTuples False <- envAllowLargeTuples env
    , NameIn GHC_Tuple fsName <- con
    , let name = unpackFS fsName
    -- TODO: A better way to detect uses of tuples than this string malarkey
    , let (front, middle, end) = (head name, init (tail name), last name)
    , front == '(', end == ')', nub middle == ","
    , length middle >= 5
    = do
        conversionWarning "Used tuple of size > 5! Daml only has Show, Eq, Ord instances for tuples of size <= 5."
        let env' = env { envAllowLargeTuples = AllowLargeTuples True }
        convertDataCon env' m con args
    -- Fully applied
    | Just (tyArgs, tmArgs) <- splitConArgs_maybe con args = do
        tyArgs <- mapM (convertType env) tyArgs
        tmArgs <- mapM (convertExpr env) tmArgs
        (, []) <$> fullyApplied env tyArgs tmArgs

    -- Partially applied, but the constructor only takes type args.
    -- In this situation there is no worker function, so we inline
    -- the constructor by introducing some type lambdas.
    | let (conTypes, conTheta, conArgs, _) = dataConSig con
    , null conTheta && null conArgs -- no value args
    = do
        kinds <- mapM (convertKind . tyVarKind) conTypes
        withTyArgs env kinds args $ \ env' types args' -> do
            (, args') <$> fullyApplied env' types []

    -- Partially applied
    | otherwise = do
        fmap (\op -> (EVal op, args)) (qual mkWorkerName (getOccText con))
  where

    fullyApplied :: Env -> [LF.Type] -> [LF.Expr] -> ConvertM LF.Expr
    fullyApplied env tyArgs tmArgs = do
        let tycon = dataConTyCon con
        qTCon <- qual (\x -> mkTypeCon [x]) (getOccText tycon)
        let tcon = TypeConApp qTCon tyArgs
            ctorName = mkVariantCon (getOccText con)
            fldNames = ctorLabels con
            xargs = (dataConName con, args)

        case classifyDataCon con of
            EnumCon -> do
                unless (null args) $ unhandled "enum constructor with arguments" xargs
                pure $ EEnumCon qTCon ctorName

            ConstraintTupleCon -> do
                pure $ EStructCon (zipExact fldNames tmArgs)

            SimpleVariantCon ->
                fmap (EVariantCon tcon ctorName) $ case tmArgs of
                    [] -> pure EUnit
                    [tmArg] -> pure tmArg
                    _ -> unhandled "constructor with more than two unnamed arguments" xargs

            ClassCon -> pure $ mkDictCon env tcon (zipExact fldNames tmArgs)

            SimpleRecordCon -> pure $ ERecCon tcon (zipExact fldNames tmArgs)

            VariantRecordCon -> do
                let recTCon = fmap (synthesizeVariantRecord ctorName) qTCon
                pure $
                    EVariantCon tcon ctorName $
                    ERecCon (TypeConApp recTCon tyArgs) (zipExact fldNames tmArgs)

    qual :: (T.Text -> n) -> T.Text -> ConvertM (Qualified n)
    qual f t
        | Just xs <- T.stripPrefix "(," t
        , T.dropWhile (== ',') xs == ")" = qDA_Types env $ f $ "Tuple" <> T.pack (show $ T.length xs + 1)
        | IgnoreWorkerPrefix t' <- t = qualify env m $ f t'

convertArg :: Env -> GHC.Arg Var -> ConvertM LF.Arg
convertArg env = \case
    Type t -> TyArg <$> convertType env t
    e -> TmArg <$> convertExpr env e

withTyArgs :: Env -> [LF.Kind] -> [LArg Var] -> (Env -> [LF.Type] -> [LArg Var] -> ConvertM (LF.Expr, [LArg Var])) -> ConvertM (LF.Expr, [LArg Var])
withTyArgs env0 kinds0 args0 cont = go env0 args0 [] kinds0
  where
    go !env !args !types [] =
        cont env (reverse types) args
    go !env !args !types (kind : kinds) =
        withTyArg env kind args $ \env' ty args' ->
            go env' args' (ty : types) kinds

withTyArg :: Env -> LF.Kind -> [LArg Var] -> (Env -> LF.Type -> [LArg Var] -> ConvertM (LF.Expr, [LArg Var])) -> ConvertM (LF.Expr, [LArg Var])
withTyArg env _ (LType t:args) cont = do
    t <- convertType env t
    cont env t args
withTyArg env k args cont = do
    let (v, env') = envFreshTypeVar "a" env
    (x, args) <- cont env' (TVar v) args
    pure (ETyLam (v, k) x, args)

withTmArg :: Env -> LF.Type -> [LArg Var] -> (LF.Expr-> [LArg Var] -> ConvertM (LF.Expr, [LArg Var])) -> ConvertM (LF.Expr, [LArg Var])
withTmArg env _ (LExpr x:args) cont = do
    x <- convertExpr env x
    cont x args
withTmArg env t args cont = do
    v <- freshTmVar
    (x, args) <- cont (EVar v) args
    pure (ETmLam (v, t) x, args)

convertLet :: Env -> Var -> GHC.Expr Var -> (Env -> ConvertM LF.Expr) -> ConvertM LF.Expr
convertLet env binder bound mkBody = do
    bound <- convertExpr env bound
    case bound of
        EVar{} -> mkBody (envInsertAlias binder bound env)
        _ -> do
            binder <- convVarWithType env binder
            body <- mkBody env
            pure $ ELet (Binding binder bound) body

-- | Convert ghc package unit id's to LF package references.
convertUnitId :: GHC.UnitId -> MS.Map GHC.UnitId DalfPackage -> UnitId -> ConvertM LF.PackageRef
convertUnitId thisUnitId _pkgMap unitId | unitId == thisUnitId = pure LF.PRSelf
convertUnitId _thisUnitId pkgMap unitId = case unitId of
  IndefiniteUnitId x -> unsupported "Indefinite unit id's." x
  DefiniteUnitId _ -> case MS.lookup unitId pkgMap of
    Just DalfPackage{..} -> pure $ LF.PRImport dalfPackageId
    Nothing -> unknown unitId pkgMap

convertAlt :: Env -> LF.Type -> Alt Var -> ConvertM GeneralisedCaseAlternative
convertAlt env ty (DEFAULT, [], x) = GCA (GCPNormal CPDefault) <$> convertExpr env x
convertAlt env ty (DataAlt con, [], x)
    | NameIn GHC_Types "True" <- con = GCA (GCPNormal (CPBool True)) <$> convertExpr env x
    | NameIn GHC_Types "False" <- con = GCA (GCPNormal (CPBool False)) <$> convertExpr env x
    | NameIn GHC_Types "[]" <- con = GCA (GCPNormal CPNil) <$> convertExpr env x
    | NameIn GHC_Tuple "()" <- con = GCA (GCPNormal CPUnit) <$> convertExpr env x
    | NameIn DA_Internal_Prelude "None" <- con
    = GCA (GCPNormal CPNone) <$> convertExpr env x

    -- Rounding mode constructors do not have built-in LF support for pattern matching,
    -- but we get the same result with equality tests.
    | NameIn GHC_Types (RoundingModeName roundingModeLit) <- con
    = GCA (GCPEquality (EBuiltin (BERoundingMode roundingModeLit))) <$> convertExpr env x

convertAlt env ty (DataAlt con, [a,b], x)
    | NameIn GHC_Types ":" <- con
    = GCA (GCPNormal (CPCons (convVar a) (convVar b))) <$> convertExpr env x
convertAlt env ty (DataAlt con, [a], x)
    | NameIn DA_Internal_Prelude "Some" <- con
    = GCA (GCPNormal (CPSome (convVar a))) <$> convertExpr env x

convertAlt env (TConApp tcon targs) alt@(DataAlt con, vs, x) = do
    let patTypeCon = tcon
        patVariant = mkVariantCon (getOccText con)
        vArg = mkVar "$arg"

    case classifyDataCon con of
        EnumCon ->
            GCA (GCPNormal (CPEnum patTypeCon patVariant)) <$> convertExpr env x

        SimpleVariantCon -> do
            when (length vs /= dataConRepArity con) $
                unsupported "Pattern match with existential type." alt
            when (length vs >= 2) $
                unsupported "Data constructor with multiple unnamed fields." alt

            let patBinder = maybe vArg convVar (listToMaybe vs)
            GCA (GCPNormal CPVariant{..}) <$> convertExpr env x

        SimpleRecordCon ->
            unhandled "unreachable case -- convertAlt with simple record constructor" ()

        VariantRecordCon -> do
            let fields = ctorLabels con
                patBinder = vArg
            case zipExactMay vs fields of
                Nothing -> unsupported "Pattern match with existential type." alt
                Just vsFlds -> do
                    x' <- convertExpr env x
                    projBinds <- mkProjBindings env (EVar vArg) (TypeConApp (synthesizeVariantRecord patVariant <$> tcon) targs) vsFlds x'
                    pure $ GCA (GCPNormal CPVariant{..}) projBinds

convertAlt _ _ x = unsupported "Case alternative of this form." x

mkProjBindings :: Env -> LF.Expr -> TypeConApp -> [(Var, FieldName)] -> LF.Expr -> ConvertM LF.Expr
mkProjBindings env recExpr recTyp vsFlds e =
  fmap (\bindings -> mkELets bindings e) $ sequence
    [ Binding <$> convVarWithType env v <*> pure (ERecProj recTyp fld recExpr)
    | (v, fld) <- vsFlds
    , not (isDeadOcc (occInfo (idInfo v)))
    ]

mkDictCon :: Env -> TypeConApp -> [(LF.FieldName, LF.Expr)] -> LF.Expr
mkDictCon env tcon fields
-- Structs must have > 0 fields, therefore we simply make a typeclass a synonym for Unit
-- if it has no fields.
    | null fields = EUnit
    | otherwise = EStructCon fields

mkDictProj :: LF.FieldName -> LF.Expr -> LF.Expr
mkDictProj = EStructProj

-- Convert a coercion @S ~ T@ to a pair of lambdas
-- @(to :: S -> T, from :: T -> S)@ in higher-order abstract syntax style.
--
-- The definition
--
-- > newtype T = MkT S
--
-- induces two coercion axioms:
--
-- (1) @S ~ T@ -- newtype construction
--
-- (2) @T ~ S@ -- newtype projection
--
-- In both cases, the newtype @T@ can also be the dictionary type of
-- a type class with a single method and no super classes.
--
-- Coercions induced by newtypes can also occur deeply nested in function
-- types or forall quantifications. We handle those cases by recursion into
-- all sub-coercions.
convertCoercion :: Env -> Coercion -> ConvertM (LF.Expr -> LF.Expr, LF.Expr -> LF.Expr)
convertCoercion env co = evalStateT (go env co) 0
  where
    go :: Env -> Coercion -> StateT Int ConvertM (LF.Expr -> LF.Expr, LF.Expr -> LF.Expr)
    go env co@(coercionKind -> Pair s t)
        | isReflCo co = pure (id, id)
        | Just (aCo, bCo) <- splitFunCo_maybe co = do
            let Pair a a' = coercionKind aCo
            (aTo, aFrom) <- go env aCo
            (bTo, bFrom) <- go env bCo
            a <- lift $ convertType env a
            a' <- lift $ convertType env a'
            x <- mkLamBinder
            let to expr = ETmLam (x, a') $ bTo $ ETmApp expr $ aFrom $ EVar x
            let from expr = ETmLam (x, a) $ bFrom $ ETmApp expr $ aTo $ EVar x
            pure (to, from)
        -- NOTE(MH): This case is commented out because we don't know how to trigger
        -- it in a test case yet. In theory it should do the right thing though.
        --   | Just (a, k_co, co') <- splitForAllCo_maybe co
        --   , isReflCo k_co
        --   = do
        --       (a, k) <- lift $ convTypeVar a
        --       (to', from') <- go env co'
        --       let to expr = ETyLam (a, k) $ to' $ ETyApp expr $ TVar a
        --       let from expr = ETyLam (a, k) $ from' $ ETyApp expr $ TVar a
        --       pure (to, from)
        -- Case (1) & (2)
        | Just (tCon, ts, field, flv) <- isSatNewTyCon s t = newtypeCoercion tCon ts field flv
        | Just (tCon, ts, field, flv) <- isSatNewTyCon t s = swap <$> newtypeCoercion tCon ts field flv
        | SymCo co' <- co = swap <$> go env co'
        | SubCo co' <- co = go env co'
        | Just (tycon, cos) <- splitTyConAppCo_maybe co = do
            s' <- lift $ convertType env s
            t' <- lift $ convertType env t
            case (s', t', cos) of
                (TOptional a, TOptional b, [co1]) -> do
                    (f,g) <- go env co1
                    f' <- mkOptionalFMap a b f
                    g' <- mkOptionalFMap b a g
                    pure (f',g')
                (TList a, TList b, [co1]) -> do
                    (f,g) <- go env co1
                    f' <- mkListFMap a b f
                    g' <- mkListFMap b a g
                    pure (f',g')
                _ -> lift $ unhandled "TyConAppCo Coercion" (tycon, cos)

        | otherwise = lift $ unhandled "Coercion" co

    mkOptionalFMap :: LF.Type -> LF.Type -> (LF.Expr -> LF.Expr) -> StateT Int ConvertM (LF.Expr -> LF.Expr)
    mkOptionalFMap _a b f = do
        y <- mkLamBinder
        pure $ \x ->
            ECase x
                [ CaseAlternative CPNone (ENone b)
                , CaseAlternative (CPSome y) (ESome b (f (EVar y)))
                ]

    mkListFMap :: LF.Type -> LF.Type -> (LF.Expr -> LF.Expr) -> StateT Int ConvertM (LF.Expr -> LF.Expr)
    mkListFMap a b f = do
        h <- mkLamBinder
        t <- mkLamBinder
        pure $ \x -> EBuiltin BEFoldr
            `ETyApp` a
            `ETyApp` TList b
            `ETmApp` (ETmLam (h, a) $ ETmLam (t, TList b) $ ECons b (f (EVar h)) (EVar t))
            `ETmApp` ENil b
            `ETmApp` x

    newtypeCoercion tCon ts field flv = do
        ts' <- lift $ mapM (convertType env) ts
        t' <- lift $ convertQualifiedTyCon env tCon
        let tcon = TypeConApp t' ts'
        pure $ if flv == ClassFlavour
           then (\expr -> mkDictCon env tcon [(field, sanitizeTo expr)], sanitizeFrom . mkDictProj field)
           else (\expr -> ERecCon tcon [(field, expr)], ERecProj tcon field)
      where
          sanitizeTo x
              -- NOTE(MH): This is DICTIONARY SANITIZATION step (3).
              | flv == ClassFlavour = ETmLam (mkVar "_", TUnit) x
              | otherwise = x
          sanitizeFrom x
              -- NOTE(MH): This is DICTIONARY SANITIZATION step (2).
              | flv == ClassFlavour = x `ETmApp` EUnit
              | otherwise = x

    mkLamBinder = do
        n <- state (dupe . succ)
        pure $ mkVar $ "$cast" <> T.pack (show n)

    isSatNewTyCon :: GHC.Type -> GHC.Type -> Maybe (TyCon, [TyCoRep.Type], FieldName, TyConFlavour)
    isSatNewTyCon s (TypeCon t ts)
        | Just data_con <- newTyConDataCon_maybe t
        , (tvs, rhs) <- newTyConRhs t
        , length ts == length tvs
        , applyTysX tvs rhs ts `eqType` s
        , [field] <- ctorLabels data_con = Just (t, ts, field, flv)
      where
        flv = tyConFlavour t
    isSatNewTyCon _ _ = Nothing

qualify :: Env -> GHC.Module -> a -> ConvertM (Qualified a)
qualify env m x = do
    unitId <- convertUnitId (envModuleUnitId env) (envPkgMap env) $ GHC.moduleUnitId m
    pure $ rewriteStableQualified env $ Qualified unitId (convertModuleName $ GHC.moduleName m) x

qDA_Types :: Env -> a -> ConvertM (Qualified a)
qDA_Types env a = do
  pkgRef <- packageNameToPkgRef env primUnitId
  pure $ rewriteStableQualified env $ Qualified pkgRef (mkModName ["DA", "Types"]) a

-- | Types of a kind not supported in Daml-LF, e.g., the DataKinds stuff from GHC.Generics
-- are translated to a special uninhabited Erased type. This allows us to easily catch these
-- cases in data-dependencies.
erasedTy :: Env -> ConvertM LF.Type
erasedTy env = do
    pkgRef <- packageNameToPkgRef env primUnitId
    pure $ TCon $ rewriteStableQualified env (Qualified pkgRef (mkModName ["DA", "Internal", "Erased"]) (mkTypeCon ["Erased"]))

-- | Type-level strings are represented in Daml-LF via the PromotedText type. This is
-- For example, the type-level string @"foo"@ will be represented by the type
-- @PromotedText {"_foo": Unit}@. This allows us to preserve all the information we need
-- to reconstruct `HasField` instances in data-dependencies without resorting to
-- name-based hacks.
--
-- Note: It's fine to put arbitrary non-empty strings in field names, because we mangle
-- the field names in daml-lf-proto to encode undesired characters. We later reconstruct
-- the original string during unmangling. See DA.Daml.LF.Mangling for more details.
promotedTextTy :: Env -> T.Text -> ConvertM LF.Type
promotedTextTy env text = do
    pkgRef <- packageNameToPkgRef env primUnitId
    pure $ TApp
        (TCon . rewriteStableQualified env $ Qualified pkgRef
            (mkModName ["DA", "Internal", "PromotedText"])
            (mkTypeCon ["PromotedText"]))
        (TStruct [(FieldName ("_" <> text), TUnit)])

qualifyLocally :: Env -> a -> Qualified a
qualifyLocally env qualObject = Qualified
  { qualPackage = PRSelf
  , qualModule = envLFModuleName env
  , qualObject
  }

-- | Rewrite an a qualified name into a reference into one of the hardcoded
-- stable packages if there is one.
rewriteStableQualified :: Env -> Qualified a -> Qualified a
rewriteStableQualified env q@(Qualified pkgRef modName obj) =
    let mbUnitId = case pkgRef of
            PRSelf -> Just (envModuleUnitId env)
            PRImport pkgId ->
                -- TODO We probably want to replace this my a more efficient lookup at some point
                fmap fst $ find (\(_, dalfPkg) -> dalfPackageId dalfPkg == pkgId) $ MS.toList $ envPkgMap env
    in case (\unitId -> MS.lookup (unitId, modName) (envStablePackages env)) =<< mbUnitId of
      Nothing -> q
      Just pkgId -> Qualified (PRImport pkgId) modName obj

convertQualifiedModuleName :: a -> Env -> GHC.Module -> ConvertM (Qualified a)
convertQualifiedModuleName x env (GHC.Module unitId moduleName) = do
  pkgRef <- unitIdToPkgRef env unitId
  let modName = convertModuleName moduleName
  pure $ rewriteStableQualified env $ Qualified
    pkgRef
    modName
    x

convertQualified :: NamedThing a => (Name -> t) -> Env -> a -> ConvertM (Qualified t)
convertQualified toT env x = do
  convertQualifiedModuleName (toT (getName x)) env (nameModule (getName x))

convertQualifiedTySyn :: NamedThing a => Env -> a -> ConvertM (Qualified TypeSynName)
convertQualifiedTySyn = convertQualified (\n -> mkTypeSyn [getOccText n])

convertQualifiedTyCon :: NamedThing a => Env -> a -> ConvertM (Qualified TypeConName)
convertQualifiedTyCon = convertQualified (\n -> mkTypeCon [getOccText n])

nameToPkgRef :: NamedThing a => Env -> a -> ConvertM LF.PackageRef
nameToPkgRef env x =
  maybe (pure LF.PRSelf) (unitIdToPkgRef env . moduleUnitId) $
    Name.nameModule_maybe $ getName x

unitIdToPkgRef :: Env -> UnitId -> ConvertM LF.PackageRef
unitIdToPkgRef env unitId =
  convertUnitId thisUnitId pkgMap unitId
  where
    thisUnitId = envModuleUnitId env
    pkgMap = envPkgMap env

packageNameToPkgRef :: Env -> UnitId -> ConvertM LF.PackageRef
packageNameToPkgRef env = convertUnitId (envModuleUnitId env) (envPkgMap env)

convertTyCon :: Env -> TyCon -> ConvertM LF.Type
convertTyCon env t
    | t == unitTyCon = pure TUnit
    | isTupleTyCon t, not (isConstraintTupleTyCon t), arity >= 2 =
        TCon <$> qDA_Types env (mkTypeCon ["Tuple" <> T.pack (show arity)])
    | t == listTyCon = pure (TBuiltin BTList)
    | t == boolTyCon = pure TBool
    | t == intTyCon || t == intPrimTyCon = pure TInt64
    | t == charTyCon = unsupported "Type GHC.Types.Char" t
    | t == liftedRepDataConTyCon = erasedTy env
    | t == typeSymbolKindCon = erasedTy env
    | NameIn GHC_Types n <- t =
        case n of
            "Text" -> pure TText
            "Numeric" -> pure (TBuiltin BTNumeric)
            "Decimal" -> pure TNumeric10
            "BigNumeric" -> pure TBigNumeric
            "RoundingMode" -> pure TRoundingMode
            _ -> defaultTyCon
    | NameIn DA_Internal_LF n <- t =
        case n of
            "Scenario" -> pure (TBuiltin BTScenario)
            "ContractId" -> pure (TBuiltin BTContractId)
            "Update" -> pure (TBuiltin BTUpdate)
            "Party" -> pure TParty
            "Date" -> pure TDate
            "Time" -> pure TTimestamp
            "TextMap" -> pure (TBuiltin BTTextMap)
            "Map" -> pure (TBuiltin BTGenMap)
            "Any" -> pure TAny
            "TypeRep" -> pure TTypeRep
            "AnyException" -> pure (TBuiltin BTAnyException)
            _ -> defaultTyCon
    | NameIn DA_Internal_Prelude "Optional" <- t = pure (TBuiltin BTOptional)
    | otherwise = defaultTyCon
    where
        arity = tyConArity t
        defaultTyCon = TCon <$> convertQualifiedTyCon env t

convertType :: Env -> GHC.Type -> ConvertM LF.Type
convertType env = go env
  where
    go :: Env -> GHC.Type -> ConvertM LF.Type
    go env o@(TypeCon t ts)
        | t == listTyCon, ts `eqTypes` [charTy] =
            pure TText
        | t == anyTyCon, [_] <- ts =
            -- used for type-zonking
            -- We translate this to Erased instead of TUnit since we do
            -- not want to translate this back to `()` for `data-dependencies`
            -- and because we never actually have values of type `Any xs` which
            -- is made explicit by translating it to an uninhabited type.
            erasedTy env

        | t == funTyCon, _:_:ts' <- ts =
            foldl' TApp TArrow <$> mapM (go env) ts'
        | NameIn DA_Internal_LF "Pair" <- t
        , [StrLitTy f1, StrLitTy f2, t1, t2] <- ts = do
            t1 <- go env t1
            t2 <- go env t2
            pure $ TStruct [(mkField f1, t1), (mkField f2, t2)]
        | tyConFlavour t == TypeSynonymFlavour =
            go env (expandTypeSynonyms o)
        | isConstraintTupleTyCon t = do
            fieldTys <- mapM (go env) ts
            let fieldNames = map mkSuperClassField [1..]
            pure $ TStruct (zip fieldNames fieldTys)
        | tyConFlavour t == ClassFlavour = do
           tySyn <- convertQualifiedTySyn env t
           TSynApp tySyn <$> mapM (go env) ts
        | otherwise =
            mkTApps <$> convertTyCon env t <*> mapM (go env) ts

    go env t
        | Just (v, t') <- splitForAllTy_maybe t
        = do
            (env', v') <- bindTypeVar env v
            TForall v' <$> go env' t'

    go env t | Just v <- getTyVar_maybe t
        = TVar . fst <$> convTypeVar env v

    go env t | Just s <- isStrLitTy t
        = promotedTextTy env (fsToText s)

    go env t | Just m <- isNumLitTy t
        = case typeLevelNatE m of
            Left TLNEOutOfBounds ->
                unsupported "type-level natural outside of supported range [0, 37]." m
            Right n ->
                pure (TNat n)

    go env t | Just (a,b) <- splitAppTy_maybe t
        = TApp <$> go env a <*> go env b

    go _ t = unhandled "Type" t


convertKind :: GHC.Kind -> ConvertM LF.Kind
convertKind x@(TypeCon t ts)
    | t == typeSymbolKindCon, null ts = pure KStar
    | t == tYPETyCon, [_] <- ts = pure KStar
    | t == runtimeRepTyCon, null ts = pure KStar
    | NameIn GHC_Types "Nat" <- getName t, null ts = pure KNat
    | t == funTyCon, [_,_,t1,t2] <- ts = do
        k1 <- convertKind t1
        k2 <- convertKind t2
        case k2 of
            KNat -> unsupported "Nat kind on the right-hand side of kind arrow." x
            _ -> pure (KArrow k1 k2)
convertKind (TyVarTy x) = convertKind $ tyVarKind x
convertKind x = unhandled "Kind" x

convNameLoc :: NamedThing a => a -> Maybe LF.SourceLoc
convNameLoc n = case nameSrcSpan (getName n) of
  -- NOTE(MH): Locations are 1-based in GHC and 0-based in Daml-LF.
  -- Hence all the @- 1@s below.
  RealSrcSpan srcSpan -> Just (convRealSrcSpan srcSpan)
  UnhelpfulSpan{} -> Nothing

convRealSrcSpan :: RealSrcSpan -> LF.SourceLoc
convRealSrcSpan srcSpan = SourceLoc
    { slocModuleRef = Nothing
    , slocStartLine = srcSpanStartLine srcSpan - 1
    , slocStartCol = srcSpanStartCol srcSpan - 1
    , slocEndLine = srcSpanEndLine srcSpan - 1
    , slocEndCol = srcSpanEndCol srcSpan - 1
    }

---------------------------------------------------------------------
-- SMART CONSTRUCTORS

defDataType :: TypeConName -> [(TypeVarName, LF.Kind)] -> DataCons -> Definition
defDataType name params constrs =
  DDataType $ DefDataType Nothing name (IsSerializable False) params constrs

defTypeSyn :: TypeSynName -> [(TypeVarName, LF.Kind)] -> LF.Type -> Definition
defTypeSyn name params ty =
  DTypeSyn $ DefTypeSyn Nothing name params ty

defValue :: NamedThing a => a -> (ExprValName, LF.Type) -> LF.Expr -> Definition
defValue loc binder@(name, lftype) body =
  DValue $ DefValue (convNameLoc loc) binder (IsTest isTest) body
  where
    isTest = case view _TForalls lftype of
      (_, LF.TScenario _)  -> True
      _ -> False

---------------------------------------------------------------------
-- UNPACK CONSTRUCTORS

-- NOTE:
--
-- * Dictionary types are converted into LF structs with one field
--   "s_1", "s_2", "s_3" ... per superclass, and one field
--   "m_foo", "m_bar", .... per method. This also applies to
--   constraint tuples, which only have superclasses.
--
-- * We treat all newtypes like records. First of all, not doing
--   this would considerably complicate converting coercions. Second, it makes
--   unpacking them faster in the current intepreter. Since the constructor
--   names of newtypes are meant to be meaningless, this is acceptable.
--
-- * We add a field for every constraint containt in the thetas.
ctorLabels :: DataCon -> [FieldName]
ctorLabels con =
    [mkField $ "$dict" <> T.pack (show i) | i <- [1 .. length thetas]] ++ conFields
  where
  thetas = dataConTheta con
  conFields
    | flv == ClassFlavour
    , tycon <- dataConTyCon con
    , Just tycls <- tyConClass_maybe tycon
    = map mkSuperClassField [1 .. length (classSCTheta tycls)]
    ++ map (mkClassMethodField . getOccText) (classMethods tycls)

    | flv == TupleFlavour Boxed || isTupleDataCon con
      -- NOTE(MH): The line below is a workaround for ghc issue
      -- https://github.com/ghc/ghc/blob/ae4f1033cfe131fca9416e2993bda081e1f8c152/compiler/types/TyCon.hs#L2030
      -- If we omit this workaround, `GHC.Tuple.Unit` gets translated into a
      -- variant rather than a record and the `SugarUnit` test will fail.
      || (getOccFS con == "Unit" && nameModule (getName con) == gHC_TUPLE)
    = map mkIndexedField [1..dataConSourceArity con]
    | flv == NewtypeFlavour && null lbls
    = [mkField "unpack"]
    | otherwise
    = map convFieldName lbls
  flv = tyConFlavour (dataConTyCon con)
  lbls = dataConFieldLabels con

---------------------------------------------------------------------
-- SIMPLE WRAPPERS

convFieldName :: FieldLbl a -> FieldName
convFieldName = mkField . fsToText . flLabel

bindTypeVar :: Env -> Var -> ConvertM (Env, (TypeVarName, LF.Kind))
bindTypeVar env v = do
    let (n, env') = envBindTypeVar v env
    k <- convertKind (tyVarKind v)
    pure (env', (n, k))

bindTypeVars :: Env -> [Var] -> ConvertM (Env, [(TypeVarName, LF.Kind)])
bindTypeVars env vs = do
    let (ns, env') = envBindTypeVars vs env
    ks <- mapM (convertKind . tyVarKind) vs
    pure (env', zipExact ns ks)

convTypeVar :: Env -> Var -> ConvertM (TypeVarName, LF.Kind)
convTypeVar env v = do
    n <- convTypeVarName env v
    k <- convertKind $ tyVarKind v
    pure (n, k)

convTypeVarName :: Env -> Var -> ConvertM TypeVarName
convTypeVarName env v = do
    case envLookupTypeVar v env of
        Nothing ->
            unhandled "Type variable not bound in conversion environment" v
        Just tv -> pure tv

convVar :: Var -> ExprVarName
convVar = mkVar . varPrettyPrint

convVarWithType :: Env -> Var -> ConvertM (ExprVarName, LF.Type)
convVarWithType env v = (convVar v,) <$> convertType env (varType v)

convVal :: Var -> ExprValName
convVal = mkVal . varPrettyPrint

convValWithType :: Env -> Var -> ConvertM (ExprValName, LF.Type)
convValWithType env v = (convVal v,) <$> convertType env (varType v)

mkPure :: Env -> GHC.Type -> GHC.Expr Var -> LF.Type -> LF.Expr -> ConvertM LF.Expr
mkPure env monad dict t x = do
    monad' <- convertType env monad
    case monad' of
      TBuiltin BTUpdate -> pure $ EUpdate (UPure t x)
      TBuiltin BTScenario -> pure $ EScenario (SPure t x)
      _ -> do
        dict' <- convertExpr env dict
        pkgRef <- packageNameToPkgRef env damlStdlib
        pure $ EVal (Qualified pkgRef (mkModName ["DA", "Internal", "Prelude"]) (mkVal "pure"))
          `ETyApp` monad'
          `ETmApp` dict'
          `ETyApp` t
          `ETmApp` x
