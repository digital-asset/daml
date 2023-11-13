-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances #-}
module DA.Daml.LF.Ast.Pretty
    ( levelHasTypes
    , levelHasKinds
    , levelHasPackageIds
    , levelHasLocations
    , (<:>)
    ) where

import Control.Lens
import Control.Lens.Ast (rightSpine)
import DA.Daml.LF.Ast.Base hiding (dataCons)
import DA.Daml.LF.Ast.Optics
import DA.Daml.LF.Ast.TypeLevelNat
import DA.Daml.LF.Ast.Util
import DA.Pretty hiding (keyword_, pretty, type_)
import Data.Foldable (toList)
import Data.Maybe (maybeToList, isJust)
import Data.NameMap qualified as NM
import Data.Ratio qualified as Ratio
import Data.Set qualified as S
import Data.Text qualified as T
import Data.Time.Clock.POSIX qualified as Clock.Posix
import Data.Time.Format qualified as Time.Format

-- NOTE(MH): We define 4 detail levels:
-- -2: Omit all type information, kind annotations, package ids and location information.
-- -1: Omit all package ids and all location information.
-- 0 (default): Omit all location information.
-- 1: Print everything.
levelHasTypes, levelHasKinds, levelHasPackageIds, levelHasLocations :: PrettyLevel -> Bool
levelHasTypes lvl = lvl >= PrettyLevel (-1)
levelHasKinds lvl = lvl >= PrettyLevel (-1)
levelHasPackageIds lvl = lvl >= PrettyLevel 0
levelHasLocations lvl = lvl >= PrettyLevel 1

infixr 6 <:>
(<:>) :: Doc ann -> Doc ann -> Doc ann
x <:> y = x <-> ":" <-> y

keyword_ :: String -> Doc ann
keyword_ = string

kind_ :: Doc ann -> Doc ann
kind_ = id

type_ :: Doc ann -> Doc ann
type_ = id

pPrintDottedName :: [T.Text] -> Doc ann
pPrintDottedName = hcat . punctuate "." . map text

instance Pretty PackageId where
    pPrint = text . unPackageId

instance Pretty ModuleName where
    pPrint = pPrintDottedName . unModuleName

instance Pretty TypeSynName where
    pPrint = pPrintDottedName . unTypeSynName

instance Pretty TypeConName where
    pPrint = pPrintDottedName . unTypeConName

instance Pretty ChoiceName where
    pPrint = text . unChoiceName

instance Pretty MethodName where
    pPrint = text . unMethodName

instance Pretty FieldName where
    pPrint = text . unFieldName

instance Pretty VariantConName where
    pPrint = text . unVariantConName

instance Pretty TypeVarName where
    pPrint = text . unTypeVarName

instance Pretty ExprVarName where
    pPrint = text . unExprVarName

instance Pretty ExprValName where
    pPrint = text . unExprValName

pPrintModuleRef :: PrettyLevel -> (PackageRef, ModuleName) -> Doc ann
pPrintModuleRef lvl (pkgRef, modName) = docPkgRef <> pPrint modName
  where
    docPkgRef = case pkgRef of
      PRSelf -> empty
      PRImport pkgId
        | levelHasPackageIds lvl -> pPrint pkgId <> ":"
        | otherwise -> empty

instance Pretty a => Pretty (Qualified a) where
    pPrintPrec lvl _prec (Qualified pkgRef modName x) =
        pPrintModuleRef lvl (pkgRef, modName) <> ":" <> pPrintPrec lvl 0 x

instance Pretty SourceLoc where
  pPrintPrec lvl _prec (SourceLoc mbModRef slin scol elin ecol) =
    hcat
    [ maybe empty (\modRef -> pPrintModuleRef lvl modRef <> ":") mbModRef
    , int slin, ":", int scol, "-", int elin, ":", int ecol
    ]

withSourceLoc :: PrettyLevel -> Maybe SourceLoc -> Doc ann -> Doc ann
withSourceLoc lvl mbLoc doc
  | Just loc <- mbLoc, levelHasLocations lvl = "@location" <> parens (pPrintPrec lvl 0 loc) $$ doc
  | otherwise = doc

precHighest, precKArrow, precTApp, precTFun, precTForall :: Rational
precHighest = 1000  -- NOTE(MH): Used for type applications in 'Expr'.
precKArrow  = 0
precTApp    = 2
precTFun    = 1
precTForall = 0

docFunArrow, docForall, docHasType :: Doc ann
docFunArrow = "->"
docForall   = "forall"
docHasType  = ":"

instance Pretty Kind where
  pPrintPrec lvl prec = \case
    KStar -> "*"
    KNat -> "nat"
    KArrow k1 k2 ->
      maybeParens (prec > precKArrow) $
        pPrintPrec lvl (succ precKArrow) k1 <-> docFunArrow <-> pPrintPrec lvl precKArrow k2

instance Pretty TypeConApp where
  pPrintPrec lvl prec = pPrintPrec lvl prec . typeConAppToType

instance Pretty BuiltinType where
  pPrint = \case
    BTInt64          -> "Int64"
    BTNumeric -> "Numeric"
    BTText           -> "Text"
    BTTimestamp      -> "Timestamp"
    BTParty          -> "Party"
    BTUnit -> "Unit"
    BTBool -> "Bool"
    BTList -> "List"
    BTUpdate -> "Update"
    BTScenario -> "Scenario"
    BTDate           -> "Date"
    BTContractId -> "ContractId"
    BTOptional -> "Optional"
    BTTextMap -> "TextMap"
    BTGenMap -> "GenMap"
    BTArrow -> parens docFunArrow
    BTAny -> "Any"
    BTTypeRep -> "TypeRep"
    BTRoundingMode -> "RoundingMode"
    BTBigNumeric -> "BigNumeric"
    BTAnyException -> "AnyException"

pPrintRecord :: Pretty a => PrettyLevel -> Doc ann -> [(FieldName, a)] -> Doc ann
pPrintRecord lvl sept fields =
  braces (sep (punctuate ";" (map pPrintField fields)))
  where
    pPrintField (name, thing) = hang (pPrint name <-> sept) 2 (pPrintPrec lvl 0 thing)

pPrintStruct :: Pretty a => PrettyLevel -> Doc ann -> [(FieldName, a)] -> Doc ann
pPrintStruct lvl sept fields =
  "<" <> sep (punctuate ";" (map pPrintField fields)) <> ">"
  where
    pPrintField (name, thing) = hang (pPrint name <-> sept) 2 (pPrintPrec lvl 0 thing)

instance Pretty Type where
  pPrintPrec lvl prec = \case
    TVar v -> pPrint v
    TCon c -> pPrintPrec lvl prec c
    TSynApp s args ->
      maybeParens (prec > precTApp) $
        pPrintPrec lvl precTApp s <-> hsep (map (pPrintPrec lvl (succ precTApp)) args)
    TApp (TApp (TBuiltin BTArrow) tx) ty ->
      maybeParens (prec > precTFun)
        (pPrintPrec lvl (succ precTFun) tx <-> docFunArrow <-> pPrintPrec lvl precTFun ty)
    TApp tf ta ->
      maybeParens (prec > precTApp) $
        pPrintPrec lvl precTApp tf <-> pPrintPrec lvl (succ precTApp) ta
    TBuiltin b -> pPrintPrec lvl prec b
    t0@TForall{} ->
      let (vs, t1) = view _TForalls t0
      in  maybeParens (prec > precTForall)
            (docForall <-> hsep (map (pPrintAndKind lvl precParam) vs) <> "."
             <-> pPrintPrec lvl precTForall t1)
    TStruct fields -> pPrintStruct lvl docHasType fields
    TNat n -> integer (fromTypeLevelNat n)

precEApp, precELam :: Rational
precEApp = 2
precELam = 0

docTmLambda, docTyLambda, docTmLambdaDot, docTyLambdaDot, docAltArrow :: Doc ann
docTmLambda = "\\"
docTyLambda = "/\\"
docTmLambdaDot = "."
docTyLambdaDot = "."
docAltArrow = "->"

prettyRounding :: RoundingModeLiteral -> String
prettyRounding = \case
  LitRoundingUp -> "ROUNDING_UP"
  LitRoundingDown -> "ROUNDING_DOWN"
  LitRoundingCeiling -> "ROUNDING_CEILING"
  LitRoundingFloor -> "ROUNDING_FLOOR"
  LitRoundingHalfUp -> "ROUNDING_HALF_UP"
  LitRoundingHalfDown -> "ROUNDING_HALF_DOWN"
  LitRoundingHalfEven -> "ROUNDING_HALF_EVEN"
  LitRoundingUnnecessary -> "ROUNDING_UNNECESSARY"

instance Pretty BuiltinExpr where
  pPrintPrec lvl prec = \case
    BEInt64 n -> integer (toInteger n)
    BENumeric n -> string (show n)
    BEText t -> string (show t) -- includes the double quotes, and escapes characters
    BEUnit -> keyword_ "unit"
    BEBool b -> keyword_ $ case b of { False -> "false"; True -> "true" }
    BERoundingMode r -> keyword_ $ prettyRounding r
    BEError -> "ERROR"
    BEAnyExceptionMessage -> "ANY_EXCEPTION_MESSAGE"
    BEEqualGeneric -> "EQUAL"
    BELessGeneric -> "LESS"
    BELessEqGeneric -> "LESS_EQ"
    BEGreaterGeneric -> "GREATER"
    BEGreaterEqGeneric -> "GREATER_EQ"
    BEEqual t     -> pPrintAppKeyword lvl prec "EQUAL"      [TyArg (TBuiltin t)]
    BELess t      -> pPrintAppKeyword lvl prec "LESS"       [TyArg (TBuiltin t)]
    BELessEq t    -> pPrintAppKeyword lvl prec "LESS_EQ"    [TyArg (TBuiltin t)]
    BEGreater t   -> pPrintAppKeyword lvl prec "GREATER"    [TyArg (TBuiltin t)]
    BEGreaterEq t -> pPrintAppKeyword lvl prec "GREATER_EQ" [TyArg (TBuiltin t)]
    BEToText t    -> pPrintAppKeyword lvl prec "TO_TEXT"    [TyArg (TBuiltin t)]
    BEContractIdToText -> "CONTRACT_ID_TO_TEXT"
    BEAddNumeric -> "ADD_NUMERIC"
    BESubNumeric -> "SUB_NUMERIC"
    BEMulNumericLegacy -> "MUL_NUMERIC_LEGACY"
    BEMulNumeric -> "MUL_NUMERIC"
    BEDivNumericLegacy -> "DIV_NUMERIC_LEGACY"
    BEDivNumeric -> "DIV_NUMERIC"
    BERoundNumeric -> "ROUND_NUMERIC"
    BECastNumericLegacy -> "CAST_NUMERIC_LEGACY"
    BECastNumeric -> "CAST_NUMERIC"
    BEShiftNumericLegacy -> "SHIFT_NUMERIC_LEGACY"
    BEShiftNumeric -> "SHIFT_NUMERIC"
    BEInt64ToNumericLegacy -> "INT64_TO_NUMERIC_LEGACY"
    BEInt64ToNumeric -> "INT64_TO_NUMERIC"
    BENumericToInt64 -> "NUMERIC_TO_INT64"
    BEEqualNumeric -> "EQUAL_NUMERIC"
    BELessEqNumeric -> "LEQ_NUMERIC"
    BELessNumeric -> "LESS_NUMERIC"
    BEGreaterEqNumeric -> "GEQ_NUMERIC"
    BEGreaterNumeric -> "GREATER_NUMERIC"
    BETextToNumericLegacy -> "TEXT_TO_NUMERIC_LEGACY"
    BETextToNumeric -> "TEXT_TO_NUMERIC"
    BENumericToText -> "NUMERIC_TO_TEXT"
    BEScaleBigNumeric -> "SCALE_BIGNUMERIC"
    BEPrecisionBigNumeric -> "PRECISION_BIGNUMERIC"
    BEAddBigNumeric -> "ADD_BIGNUMERIC"
    BESubBigNumeric -> "SUB_BIGNUMERIC"
    BEMulBigNumeric -> "MUl_BIGNUMERIC"
    BEDivBigNumeric -> "DIV_BIGNUMERIC"
    BEShiftRightBigNumeric -> "SHIFT_RIGHT_BIGNUMERIC"
    BEBigNumericToNumericLegacy -> "BIGNUMERIC_TO_NUMERIC_LEGACY"
    BEBigNumericToNumeric -> "BIGNUMERIC_TO_NUMERIC"
    BENumericToBigNumeric -> "NUMERIC_TO_BIGNUMERIC"
    BEAddInt64 -> "ADD_INT64"
    BESubInt64 -> "SUB_INT64"
    BEMulInt64 -> "MUL_INT64"
    BEDivInt64 -> "DIV_INT64"
    BEModInt64 -> "MOD_INT64"
    BEExpInt64 -> "EXP_INT64"
    BEFoldl -> "FOLDL"
    BEFoldr -> "FOLDR"
    BETextMapEmpty -> "TEXTMAP_EMPTY"
    BETextMapInsert -> "TEXTMAP_INSERT"
    BETextMapLookup -> "TEXTMAP_LOOKUP"
    BETextMapDelete -> "TEXTMAP_DELETE"
    BETextMapSize -> "TEXTMAP_SIZE"
    BETextMapToList -> "TEXTMAP_TO_LIST"
    BEGenMapEmpty -> "GENMAP_EMPTY"
    BEGenMapInsert -> "GENMAP_INSERT"
    BEGenMapLookup -> "GENMAP_LOOKUP"
    BEGenMapDelete -> "GENMAP_DELETE"
    BEGenMapSize -> "GENMAP_SIZE"
    BEGenMapKeys -> "GENMAP_KEYS"
    BEGenMapValues -> "GENMAP_VALUES"
    BEEqualList -> "EQUAL_LIST"
    BEAppendText -> "APPEND_TEXT"
    BETimestamp ts -> text (timestampToText ts)
    BEDate date -> text (dateToText date)
    BETimestampToUnixMicroseconds -> "TIMESTAMP_TO_UNIX_MICROSECONDS"
    BEUnixMicrosecondsToTimestamp -> "UNIX_MICROSECONDS_TO_TIMESTAMP"
    BEDateToUnixDays -> "DATE_TO_UNIX_DAYS"
    BEUnixDaysToDate -> "UNIX_DAYS_TO_DATE"
    BEExplodeText -> "EXPLODE_TEXT"
    BEImplodeText -> "IMPLODE_TEXT"
    BESha256Text -> "SHA256_TEXT"
    BETrace -> "TRACE"
    BEEqualContractId -> "EQUAL_CONTRACT_ID"
    BETextToParty -> "TEXT_TO_PARTY"
    BETextToInt64 -> "TEXT_TO_INT64"
    BEPartyToQuotedText -> "PARTY_TO_QUOTED_TEXT"
    BETextToCodePoints -> "TEXT_TO_CODE_POINTS"
    BECodePointsToText -> "CODE_POINTS_TO_TEXT"
    BECoerceContractId -> "COERCE_CONTRACT_ID"
    BETypeRepTyConName -> "TYPE_REP_TYCON_NAME"

    where
      epochToText fmt secs =
        T.pack $
        Time.Format.formatTime Time.Format.defaultTimeLocale fmt $
        Clock.Posix.posixSecondsToUTCTime (fromRational secs)

      timestampToText micros =
        epochToText "%0Y-%m-%dT%T%QZ" (toInteger micros Ratio.% (10 ^ (6 :: Integer)))

      dateToText days = epochToText "%0Y-%m-%d" ((toInteger days * 24 * 60 * 60) Ratio.% 1)

precBinding, precParam :: Rational
precBinding = 0
precParam = 1

pPrintAndKind :: Pretty a => PrettyLevel -> Rational -> (a, Kind) -> Doc ann
pPrintAndKind lvl prec (v, k)
  | levelHasKinds lvl = maybeParens (prec > precBinding) $
      pPrintPrec lvl 0 v <-> docHasType <-> kind_ (pPrintPrec lvl 0 k)
  | otherwise = pPrintPrec lvl 0 v

pPrintAndType :: Pretty a => PrettyLevel -> Rational -> (a, Type) -> Doc ann
pPrintAndType lvl prec (x, t)
  | levelHasTypes lvl = maybeParens (prec > precBinding) $
      pPrintPrec lvl 0 x <-> docHasType <-> type_ (pPrintPrec lvl 0 t)
  | otherwise = pPrintPrec lvl 0 x

instance Pretty CasePattern where
  pPrintPrec lvl _prec = \case
    CPVariant tcon con var ->
      pPrintPrec lvl 0 tcon <> ":" <> pPrint con
      <-> pPrint var
    CPEnum tcon con -> pPrintPrec lvl 0 tcon <> ":" <> pPrint con
    CPUnit -> keyword_ "unit"
    CPBool b -> keyword_ $ case b of { False -> "false"; True -> "true" }
    CPNil -> keyword_ "nil"
    CPCons hdVar tlVar -> keyword_ "cons" <-> pPrint hdVar <-> pPrint tlVar
    CPDefault -> keyword_ "default"
    CPNone -> keyword_ "none"
    CPSome bodyVar -> keyword_ "some" <-> pPrint bodyVar

instance Pretty CaseAlternative where
  pPrintPrec lvl _prec (CaseAlternative pat expr) =
    hang (pPrintPrec lvl 0 pat <-> docAltArrow) 2 (pPrintPrec lvl 0 expr)

instance Pretty Binding where
  pPrintPrec lvl _prec (Binding binder expr) =
    hang (pPrintAndType lvl precBinding binder <-> "=") 2 (pPrintPrec lvl 0 expr)

pPrintTyArg :: PrettyLevel -> Type -> Doc ann
pPrintTyArg lvl t
  | levelHasTypes lvl = type_ ("@" <> pPrintPrec lvl precHighest t)
  | otherwise = empty

pPrintTmArg :: PrettyLevel -> Expr -> Doc ann
pPrintTmArg lvl = pPrintPrec lvl (succ precEApp)

tplArg :: Qualified TypeConName -> Arg
tplArg tpl = TyArg (TCon tpl)

interfaceArg :: Qualified TypeConName -> Arg
interfaceArg tpl = TyArg (TCon tpl)

methodArg :: MethodName -> Arg
methodArg = TmArg . EVar . ExprVarName . unMethodName

instance Pretty Arg where
  pPrintPrec lvl _prec = \case
    TmArg e -> pPrintTmArg lvl e
    TyArg t -> pPrintTyArg lvl t

pPrintAppDoc :: PrettyLevel -> Rational -> Doc ann -> [Arg] -> Doc ann
pPrintAppDoc lvl prec d as = maybeParens (prec > precEApp) $
  sep (d : map (nest 2 . pPrintPrec lvl (succ precEApp)) as)

pPrintAppKeyword :: PrettyLevel -> Rational -> String -> [Arg] -> Doc ann
pPrintAppKeyword lvl prec kw = pPrintAppDoc lvl prec (keyword_ kw)

pPrintApp :: PrettyLevel -> Rational -> Expr -> [Arg] -> Doc ann
pPrintApp lvl prec f = pPrintAppDoc lvl prec (pPrintPrec lvl precEApp f)

instance Pretty Update where
  pPrintPrec lvl prec = \case
    UPure typ arg ->
      pPrintAppKeyword lvl prec "upure" [TyArg typ, TmArg arg]
    upd@UBind{} -> maybeParens (prec > precELam) $
      let (binds, body) = view (rightSpine (unlocate $ _EUpdate . _UBind)) (EUpdate upd)
      in  keyword_ "ubind" <-> vcat (map (pPrintPrec lvl precELam) binds)
          $$ keyword_ "in" <-> pPrintPrec lvl precELam body
    UCreate tpl arg ->
      pPrintAppKeyword lvl prec "create" [tplArg tpl, TmArg arg]
    UCreateInterface interface arg ->
      pPrintAppKeyword lvl prec "create_interface" [interfaceArg interface, TmArg arg]
    UExercise tpl choice cid arg ->
      -- NOTE(MH): Converting the choice name into a variable is a bit of a hack.
      pPrintAppKeyword lvl prec "exercise"
      [tplArg tpl, TmArg (EVar (ExprVarName (unChoiceName choice))), TmArg cid, TmArg arg]
    USoftExercise tpl choice cid arg ->
      pPrintAppKeyword lvl prec "soft_exercise"
      [tplArg tpl, TmArg (EVar (ExprVarName (unChoiceName choice))), TmArg cid, TmArg arg]
    UDynamicExercise tpl choice cid arg ->
      -- NOTE(MH): Converting the choice name into a variable is a bit of a hack.
      pPrintAppKeyword lvl prec "dynamic_exercise"
      [tplArg tpl, TmArg (EVar (ExprVarName (unChoiceName choice))), TmArg cid, TmArg arg]
    UExerciseInterface interface choice cid arg guard ->
      let -- We distinguish guarded and unguarded exercises by Just/Nothing in guard
          keyword = "exercise_interface" ++ if isJust guard then "_guarded" else ""
          guardArg = TmArg <$> maybeToList guard
          -- NOTE(MH): Converting the choice name into a variable is a bit of a hack.
      in
      pPrintAppKeyword lvl prec keyword $
      [interfaceArg interface, TmArg (EVar (ExprVarName (unChoiceName choice))), TmArg cid, TmArg arg] ++ guardArg
    UExerciseByKey tpl choice key arg ->
      pPrintAppKeyword lvl prec "exercise_by_key"
      [tplArg tpl, TmArg (EVar (ExprVarName (unChoiceName choice))), TmArg key, TmArg arg]
    UFetch tpl cid ->
      pPrintAppKeyword lvl prec "fetch" [tplArg tpl, TmArg cid]
    USoftFetch tpl cid ->
      pPrintAppKeyword lvl prec "soft_fetch" [tplArg tpl, TmArg cid]
    UFetchInterface interface cid ->
      pPrintAppKeyword lvl prec "fetch_interface" [interfaceArg interface, TmArg cid]
    UGetTime ->
      keyword_ "get_time"
    UEmbedExpr typ e ->
      pPrintAppKeyword lvl prec "uembed_expr" [TyArg typ, TmArg e]
    UFetchByKey RetrieveByKey{..} ->
      pPrintAppKeyword lvl prec "ufetch_by_key" [tplArg retrieveByKeyTemplate, TmArg retrieveByKeyKey]
    ULookupByKey RetrieveByKey{..} ->
      pPrintAppKeyword lvl prec "ulookup_by_key" [tplArg retrieveByKeyTemplate, TmArg retrieveByKeyKey]
    UTryCatch t e1 x e2 -> keyword_ "try" <-> pPrintTyArg lvl t <-> pPrintTmArg lvl e1
      <-> keyword_ "catch" <-> pPrintPrec lvl precParam x <-> keyword_ "." <-> pPrintTmArg lvl e2

instance Pretty Scenario where
  pPrintPrec lvl prec = \case
    SPure typ arg ->
      pPrintAppKeyword lvl prec "spure" [TyArg typ, TmArg arg]
    scen@SBind{} -> maybeParens (prec > precELam) $
      let (binds, body) = view (rightSpine (_EScenario . _SBind)) (EScenario scen)
      in  keyword_ "sbind" <-> vcat (map (pPrintPrec lvl precELam) binds)
          $$ keyword_ "in" <-> pPrintPrec lvl precELam body
    SCommit typ actor upd ->
      pPrintAppKeyword lvl prec "commit" [TyArg typ, TmArg actor, TmArg upd]
    SMustFailAt typ actor upd ->
      pPrintAppKeyword lvl prec "must_fail_at" [TyArg typ, TmArg actor, TmArg upd]
    SPass delta ->
      pPrintAppKeyword lvl prec "pass" [TmArg delta]
    SGetTime ->
      keyword_ "get_time"
    SGetParty name ->
      pPrintAppKeyword lvl prec "get_party" [TmArg name]
    SEmbedExpr typ e ->
      pPrintAppKeyword lvl prec "sembed_expr" [TyArg typ, TmArg e]

instance Pretty Expr where
  pPrintPrec lvl prec = \case
    EVar x -> pPrint x
    EVal z -> pPrintPrec lvl prec z
    EBuiltin b -> pPrintPrec lvl prec b
    ERecCon (TypeConApp tcon targs) fields ->
      maybeParens (prec > precEApp) $
        sep $
          pPrintPrec lvl precEApp tcon
          : map (nest 2 . pPrintTyArg lvl) targs
          ++ [nest 2 (pPrintRecord lvl "=" fields)]
    ERecProj (TypeConApp tcon targs) field rec ->
      pPrintAppDoc lvl prec
        (pPrintPrec lvl precEApp tcon <> "." <> pPrint field)
        (map TyArg targs ++ [TmArg rec])
    ERecUpd (TypeConApp tcon targs) field record update ->
      maybeParens (prec > precEApp) $
        sep $
          pPrintPrec lvl precEApp tcon
          : map (nest 2 . pPrintTyArg lvl) targs
          ++ [nest 2 (braces updDoc)]
      where
        updDoc = sep
          [ pPrintPrec lvl 0 record
          , keyword_ "with"
          , hang (pPrint field <-> "=") 2 (pPrintPrec lvl 0 update)
          ]
    EVariantCon (TypeConApp tcon targs) con arg ->
      pPrintAppDoc lvl prec
        (pPrintPrec lvl precEApp tcon <> ":" <> pPrint con)
        (map TyArg targs ++ [TmArg arg])
    EEnumCon tcon con ->
      pPrintPrec lvl precEApp tcon <> ":" <> pPrint con
    EStructCon fields ->
      pPrintStruct lvl "=" fields
    EStructProj field expr -> pPrintPrec lvl precHighest expr <> "." <> pPrint field
    EStructUpd field struct update ->
          "<" <> updDoc <> ">"
      where
        updDoc = sep
          [ pPrintPrec lvl 0 struct
          , keyword_ "with"
          , hang (pPrint field <-> "=") 2 (pPrintPrec lvl 0 update)
          ]
    e@ETmApp{} -> uncurry (pPrintApp lvl prec) (e ^. _EApps)
    e@ETyApp{} -> uncurry (pPrintApp lvl prec) (e ^. _EApps)
    e0@ETmLam{} -> maybeParens (prec > precELam) $
      let (bs, e1) = view (rightSpine (unlocate _ETmLam)) e0
      in  hang (docTmLambda <> hsep (map (pPrintAndType lvl precParam) bs) <> docTmLambdaDot)
            2 (pPrintPrec lvl precELam e1)
    e0@ETyLam{}
      | levelHasTypes lvl ->
        maybeParens (prec > precELam) $
          hang (docTyLambda <> hsep (map (pPrintAndKind lvl precParam) ts) <> docTyLambdaDot)
            2 (pPrintPrec lvl precELam e1)
      | otherwise -> pPrintPrec lvl prec e1
      where
        (ts, e1) = view (rightSpine (unlocate _ETyLam)) e0
    ECase scrut alts -> maybeParens (prec > precEApp) $
      keyword_ "case" <-> pPrintPrec lvl 0 scrut <-> keyword_ "of"
      $$ nest 2 (vcat (map (pPrintPrec lvl 0) alts))
    e0@ELet{} -> maybeParens (prec > precELam) $
      let (binds, e1) = view (rightSpine (unlocate _ELet)) e0
      in  keyword_ "let" <-> vcat (map (pPrintPrec lvl 0) binds)
          $$ keyword_ "in" <-> pPrintPrec lvl 0 e1
    ENil elemType ->
      pPrintAppKeyword lvl prec "nil" [TyArg elemType]
    ECons elemType headExpr tailExpr ->
      pPrintAppKeyword lvl prec "cons" [TyArg elemType, TmArg headExpr, TmArg tailExpr]
    EUpdate upd -> pPrintPrec lvl prec upd
    EScenario scen -> pPrintPrec lvl prec scen
    ELocation loc x
        | levelHasLocations lvl -> pPrintAppDoc lvl prec ("@location" <> parens (pPrintPrec lvl 0 loc)) [TmArg x]
        | otherwise -> pPrintPrec lvl prec x
    ESome typ body -> pPrintAppKeyword lvl prec "some" [TyArg typ, TmArg body]
    ENone typ -> pPrintAppKeyword lvl prec "none" [TyArg typ]
    EToAny ty body -> pPrintAppKeyword lvl prec "to_any" [TyArg ty, TmArg body]
    EFromAny ty body -> pPrintAppKeyword lvl prec "from_any" [TyArg ty, TmArg body]
    ETypeRep ty -> pPrintAppKeyword lvl prec "type_rep" [TyArg ty]
    EToAnyException ty val -> pPrintAppKeyword lvl prec "to_any_exception"
        [TyArg ty, TmArg val]
    EFromAnyException ty val -> pPrintAppKeyword lvl prec "from_any_exception"
        [TyArg ty, TmArg val]
    EThrow ty1 ty2 val -> pPrintAppKeyword lvl prec "throw"
        [TyArg ty1, TyArg ty2, TmArg val]
    EToInterface ty1 ty2 expr -> pPrintAppKeyword lvl prec "to_interface"
        [interfaceArg ty1, tplArg ty2, TmArg expr]
    EFromInterface ty1 ty2 expr -> pPrintAppKeyword lvl prec "from_interface"
        [interfaceArg ty1, tplArg ty2, TmArg expr]
    EUnsafeFromInterface ty1 ty2 expr1 expr2 -> pPrintAppKeyword lvl prec "unsafe_from_interface"
        [interfaceArg ty1, tplArg ty2, TmArg expr1, TmArg expr2]
    ECallInterface ty mth expr -> pPrintAppKeyword lvl prec "call_interface"
        [interfaceArg ty, methodArg mth, TmArg expr]
    EToRequiredInterface ty1 ty2 expr -> pPrintAppKeyword lvl prec "to_required_interface"
        [interfaceArg ty1, interfaceArg ty2, TmArg expr]
    EFromRequiredInterface ty1 ty2 expr -> pPrintAppKeyword lvl prec "from_required_interface"
        [interfaceArg ty1, interfaceArg ty2, TmArg expr]
    EUnsafeFromRequiredInterface ty1 ty2 expr1 expr2 -> pPrintAppKeyword lvl prec "unsafe_from_required_interface"
        [interfaceArg ty1, interfaceArg ty2, TmArg expr1, TmArg expr2]
    EInterfaceTemplateTypeRep ty expr -> pPrintAppKeyword lvl prec "interface_template_type_rep"
        [interfaceArg ty, TmArg expr]
    ESignatoryInterface ty expr -> pPrintAppKeyword lvl prec "signatory_interface"
        [interfaceArg ty, TmArg expr]
    EObserverInterface ty expr -> pPrintAppKeyword lvl prec "observer_interface"
        [interfaceArg ty, TmArg expr]
    EViewInterface iface expr -> pPrintAppKeyword lvl prec "view"
        [interfaceArg iface, TmArg expr]
    EChoiceController tpl ch expr1 expr2 -> pPrintAppKeyword lvl prec "choice_controller"
        [TyArg (TCon tpl), TmArg (EVar (ExprVarName (unChoiceName ch))), TmArg expr1, TmArg expr2]
    EChoiceObserver tpl ch expr1 expr2 -> pPrintAppKeyword lvl prec "choice_observer"
        [TyArg (TCon tpl), TmArg (EVar (ExprVarName (unChoiceName ch))), TmArg expr1, TmArg expr2]
    EExperimental name _ ->  pPrint $ "$" <> name

instance Pretty DefTypeSyn where
  pPrintPrec lvl _prec (DefTypeSyn mbLoc syn params typ) =
    withSourceLoc lvl mbLoc $ (keyword_ "synonym" <-> lhsDoc) $$ nest 2 (pPrintPrec lvl 0 typ)
    where
      lhsDoc = pPrint syn <-> hsep (map (pPrintAndKind lvl precParam) params) <-> "="

instance Pretty DefException where
  pPrintPrec lvl _prec (DefException mbLoc tycon msg) =
    withSourceLoc lvl mbLoc
      $ (keyword_ "exception" <-> pPrint tycon <-> "where")
      $$ nest 2 ("message" <-> pPrintPrec lvl 0 msg)

instance Pretty DefDataType where
  pPrintPrec lvl _prec (DefDataType mbLoc tcon (IsSerializable serializable) params dataCons) =
    withSourceLoc lvl mbLoc $ case dataCons of
    DataRecord fields ->
      hang (keyword_ "record" <-> lhsDoc) 2 (pPrintRecord lvl docHasType fields)
    DataVariant variants ->
      (keyword_ "variant" <-> lhsDoc) $$ nest 2 (vcat (map pPrintVariantCon variants))
    DataEnum enums ->
      (keyword_ "enum" <-> lhsDoc) $$ nest 2 (vcat (map pPrintEnumCon enums))
    DataInterface ->
      keyword_ "interface" <-> pPrint tcon
    where
      lhsDoc =
        serializableDoc <-> pPrint tcon <-> hsep (map (pPrintAndKind lvl precParam) params) <-> "="
      serializableDoc = if serializable then "@serializable" else empty
      pPrintVariantCon (name, typ) =
        "|" <-> pPrint name <-> pPrintPrec lvl precHighest typ
      pPrintEnumCon name = "|" <-> pPrint name

instance Pretty DefValue where
  pPrintPrec lvl _prec (DefValue mbLoc binder (IsTest isTest) body) =
    withSourceLoc lvl mbLoc $
    vcat
      [ hang (keyword_ kind <-> pPrintAndType lvl precBinding binder <-> "=") 2 (pPrintPrec lvl 0 body) ]
    where
      kind = if isTest then "test" else "def"

pPrintTemplateChoice ::
  PrettyLevel -> ModuleName -> TypeConName -> TemplateChoice -> Doc ann
pPrintTemplateChoice lvl modName tpl (TemplateChoice mbLoc name isConsuming controllers observers authorizers selfBinder argBinder retType update) =
  withSourceLoc lvl mbLoc $
    vcat
    [ hsep
      [ keyword_ (if isConsuming then "consuming" else "non-consuming")
      , keyword_ "choice"
      , pPrint name
      , pPrintAndType lvl precParam (selfBinder, TContractId (TCon (Qualified PRSelf modName tpl)))
      , pPrintAndType lvl precParam argBinder
      , if levelHasTypes lvl then docHasType <-> pPrintPrec lvl 0 retType else empty
      ]
    , nest 2 (keyword_ "controller" <-> pPrintPrec lvl 0 controllers)
    , nest 2 (keyword_ "observer" <-> pPrintPrec lvl 0 observers)
    , nest 2 (keyword_ "authority" <-> pPrintPrec lvl 0 authorizers)
    , nest 2 (keyword_ "do" <-> pPrintPrec lvl 0 update)
    ]

pPrintTemplate ::
  PrettyLevel -> ModuleName -> Template -> Doc ann
pPrintTemplate lvl modName (Template mbLoc tpl param precond signatories observers agreement choices mbKey implements) =
  withSourceLoc lvl mbLoc $
    keyword_ "template" <-> pPrint tpl <-> pPrint param
    <-> keyword_ "where"
    $$ nest 2 (vcat ([signatoriesDoc, observersDoc, precondDoc, agreementDoc] ++ implementsDoc ++ mbKeyDoc ++ choiceDocs))
    where
      signatoriesDoc = keyword_ "signatory" <-> pPrintPrec lvl 0 signatories
      observersDoc = keyword_ "observer" <-> pPrintPrec lvl 0 observers
      precondDoc = keyword_ "ensure" <-> pPrintPrec lvl 0 precond
      agreementDoc = hang (keyword_ "agreement") 2 (pPrintPrec lvl 0 agreement)
      choiceDocs = map (pPrintTemplateChoice lvl modName tpl) (NM.toList choices)
      mbKeyDoc = toList $ do
        key <- mbKey
        return $ vcat
          [ keyword_ "key" <-> if levelHasTypes lvl then pPrintPrec lvl 0 (tplKeyType key) else empty
          , nest 2 (keyword_ "body" <-> pPrintPrec lvl 0 (tplKeyBody key))
          , nest 2 (keyword_ "maintainers" <-> pPrintPrec lvl 0 (tplKeyMaintainers key))
          ]
      implementsDoc =
        [ pPrintInterfaceInstance lvl (InterfaceInstanceHead interface qTpl) body loc
        | TemplateImplements interface body loc <- NM.toList implements
        ]
      qTpl = Qualified PRSelf modName tpl

pPrintInterfaceInstance :: PrettyLevel -> InterfaceInstanceHead -> InterfaceInstanceBody -> Maybe SourceLoc -> Doc ann
pPrintInterfaceInstance lvl head body mLoc =
  withSourceLoc lvl mLoc $
    hang
      (pPrintInterfaceInstanceHead lvl head)
      2
      (pPrintInterfaceInstanceBody lvl body)

pPrintInterfaceInstanceBody :: PrettyLevel -> InterfaceInstanceBody -> Doc ann
pPrintInterfaceInstanceBody lvl (InterfaceInstanceBody methods _view) =
  vcat $ map (pPrintInterfaceInstanceMethod lvl) (NM.toList methods)

pPrintInterfaceInstanceMethod :: PrettyLevel -> InterfaceInstanceMethod -> Doc ann
pPrintInterfaceInstanceMethod lvl (InterfaceInstanceMethod name expr) =
    pPrintPrec lvl 0 name <-> keyword_ "=" <-> pPrintPrec lvl 0 expr

pPrintInterfaceInstanceHead :: PrettyLevel -> InterfaceInstanceHead -> Doc ann
pPrintInterfaceInstanceHead lvl InterfaceInstanceHead {..} =
  hsep
    [ keyword_ "interface instance"
    , pPrintPrec lvl 0 iiInterface
    , keyword_ "for"
    , pPrintPrec lvl 0 iiTemplate
    ]

instance Pretty InterfaceInstanceHead where
  pPrintPrec lvl _prec = pPrintInterfaceInstanceHead lvl

pPrintInterfaceMethod ::
  PrettyLevel -> InterfaceMethod -> Doc ann
pPrintInterfaceMethod lvl InterfaceMethod {ifmLocation, ifmName, ifmType} =
  withSourceLoc lvl ifmLocation $
    keyword_ "method" <-> pPrintAndType lvl 0 (ifmName, ifmType)

pPrintDefInterface :: PrettyLevel -> ModuleName -> DefInterface -> Doc ann
pPrintDefInterface lvl modName defInterface =
  withSourceLoc lvl intLocation $
    hang header 2 body
  where
    DefInterface
      { intLocation
      , intName
      , intRequires
      , intParam
      , intView
      , intMethods
      , intChoices
      , intCoImplements
      } = defInterface

    header = hsep
      [ keyword_ "interface"
      , pPrint intName
      , pPrint intParam
      , requiresDoc
      , keyword_ "where"
      ]

    body = vcat
      [ viewDoc
      , vcat methodDocs
      , vcat choiceDocs
      , vcat interfaceInstanceDocs
      ]

    requiresDoc = case S.toList intRequires of
      [] -> mempty
      reqs -> keyword_ "requires" <-> hsep (punctuate comma (pPrint <$> reqs))

    viewDoc = keyword_ "viewtype" <-> pPrint intView
    methodDocs = map (pPrintInterfaceMethod lvl) (NM.toList intMethods)
    choiceDocs = map (pPrintTemplateChoice lvl modName intName) (NM.toList intChoices)
    interfaceInstanceDocs =
      [ pPrintInterfaceInstance lvl (InterfaceInstanceHead qIntName template) body loc
      | InterfaceCoImplements template body loc <- NM.toList intCoImplements
      ]
    qIntName = Qualified PRSelf modName intName

pPrintFeatureFlags :: FeatureFlags -> Doc ann
pPrintFeatureFlags FeatureFlags = mempty

instance Pretty Module where
  pPrintPrec lvl _prec (Module modName _path flags synonyms dataTypes values templates exceptions interfaces) =
    vcat $
      pPrintFeatureFlags flags
      : (keyword_ "module" <-> pPrint modName <-> keyword_ "where")
      : concat
        [ map (pPrintPrec lvl 0) (NM.toList dataTypes)
        , map (pPrintPrec lvl 0) (NM.toList synonyms)
        , map (pPrintPrec lvl 0) (NM.toList values)
        , map (pPrintTemplate lvl modName) (NM.toList templates)
        , map (pPrintPrec lvl 0) (NM.toList exceptions)
        , map (pPrintDefInterface lvl modName) (NM.toList interfaces)
        ]

instance Pretty PackageName where
    pPrint = pPrint . unPackageName

instance Pretty PackageVersion where
    pPrint = pPrint . unPackageVersion

instance Pretty PackageMetadata where
    pPrint (PackageMetadata name version upgradedPid) =
      pPrint name
        <> "-" <> pPrint version
        <> maybe empty (\pid -> "-[upgrades=" <> pPrint pid <> "]") upgradedPid

instance Pretty Package where
  pPrintPrec lvl _prec (Package version modules metadata) =
    vcat $
      "daml-lf" <-> pPrintPrec lvl 0 version
      : maybe empty (\m -> "metadata" <-> pPrintPrec lvl 0 m) metadata
      : map (\m -> "" $-$ pPrintPrec lvl 0 m) (NM.toList modules)
