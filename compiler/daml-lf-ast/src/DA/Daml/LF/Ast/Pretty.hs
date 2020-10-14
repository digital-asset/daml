-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances  #-}
module DA.Daml.LF.Ast.Pretty(
    (<:>)
    ) where

import qualified Data.Ratio                 as Ratio
import           Control.Lens
import           Control.Lens.Ast   (rightSpine)
import qualified Data.NameMap as NM
import qualified Data.Text          as T
import qualified Data.Time.Clock.POSIX      as Clock.Posix
import qualified Data.Time.Format           as Time.Format
import           Data.Foldable (toList)

import           DA.Daml.LF.Ast.Base hiding (dataCons)
import           DA.Daml.LF.Ast.TypeLevelNat
import           DA.Daml.LF.Ast.Util
import           DA.Daml.LF.Ast.Optics
import           DA.Pretty hiding (keyword_, pretty, type_)

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
    BTDecimal        -> "Decimal"
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

instance Pretty PartyLiteral where
  pPrint = quotes . text . unPartyLiteral

instance Pretty BuiltinExpr where
  pPrintPrec lvl prec = \case
    BEInt64 n -> integer (toInteger n)
    BEDecimal dec -> string (show dec)
    BENumeric n -> string (show n)
    BEText t -> string (show t) -- includes the double quotes, and escapes characters
    BEParty p -> pPrint p
    BEUnit -> keyword_ "unit"
    BEBool b -> keyword_ $ case b of { False -> "false"; True -> "true" }
    BEError -> "ERROR"
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
    BEToTextContractId -> "TO_TEXT_CONTRACT_ID"
    BEAddDecimal -> "ADD_DECIMAL"
    BESubDecimal -> "SUB_DECIMAL"
    BEMulDecimal -> "MUL_DECIMAL"
    BEDivDecimal -> "DIV_DECIMAL"
    BERoundDecimal -> "ROUND_DECIMAL"
    BEAddNumeric -> "ADD_NUMERIC"
    BESubNumeric -> "SUB_NUMERIC"
    BEMulNumeric -> "MUL_NUMERIC"
    BEDivNumeric -> "DIV_NUMERIC"
    BERoundNumeric -> "ROUND_NUMERIC"
    BECastNumeric -> "CAST_NUMERIC"
    BEShiftNumeric -> "SHIFT_NUMERIC"
    BEInt64ToNumeric -> "INT64_TO_NUMERIC"
    BENumericToInt64 -> "NUMERIC_TO_INT64"
    BEEqualNumeric -> "EQUAL_NUMERIC"
    BELessEqNumeric -> "LEQ_NUMERIC"
    BELessNumeric -> "LESS_NUMERIC"
    BEGreaterEqNumeric -> "GEQ_NUMERIC"
    BEGreaterNumeric -> "GREATER_NUMERIC"
    BENumericFromText -> "FROM_TEXT_NUMERIC"
    BEToTextNumeric -> "TO_TEXT_NUMERIC"
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
    BEInt64ToDecimal -> "INT64_TO_DECIMAL"
    BEDecimalToInt64 -> "DECIMAL_TO_INT64"
    BETimestampToUnixMicroseconds -> "TIMESTAMP_TO_UNIX_MICROSECONDS"
    BEUnixMicrosecondsToTimestamp -> "UNIX_MICROSECONDS_TO_TIMESTAMP"
    BEDateToUnixDays -> "DATE_TO_UNIX_DAYS"
    BEUnixDaysToDate -> "UNIX_DAYS_TO_DATE"
    BEExplodeText -> "EXPLODE_TEXT"
    BEImplodeText -> "IMPLODE_TEXT"
    BESha256Text -> "SHA256_TEXT"
    BETrace -> "TRACE"
    BEEqualContractId -> "EQUAL_CONTRACT_ID"
    BEPartyFromText -> "FROM_TEXT_PARTY"
    BEInt64FromText -> "FROM_TEXT_INT64"
    BEDecimalFromText -> "FROM_TEXT_DECIMAL"
    BEPartyToQuotedText -> "PARTY_TO_QUOTED_TEXT"
    BETextToCodePoints -> "TEXT_TO_CODE_POINTS"
    BETextFromCodePoints -> "TEXT_FROM_CODE_POINTS"
    BECoerceContractId -> "COERCE_CONTRACT_ID"
    BETextToUpper -> "TEXT_TO_UPPER"
    BETextToLower -> "TEXT_TO_LOWER"
    BETextSlice -> "TEXT_SLICE"
    BETextSliceIndex -> "TEXT_SLICE_INDEX"
    BETextContainsOnly -> "TEXT_CONTAINS_ONLY"
    BETextReplicate -> "TEXT_REPLICATE"
    BETextSplitOn -> "TEXT_SPLIT_ON"
    BETextIntercalate -> "TEXT_INTERCALATE"

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
    UExercise tpl choice cid Nothing arg ->
      -- NOTE(MH): Converting the choice name into a variable is a bit of a hack.
      pPrintAppKeyword lvl prec "exercise"
      [tplArg tpl, TmArg (EVar (ExprVarName (unChoiceName choice))), TmArg cid, TmArg arg]
    UExercise tpl choice cid (Just actor) arg ->
      -- NOTE(MH): Converting the choice name into a variable is a bit of a hack.
      pPrintAppKeyword lvl prec "exercise_with_actors"
      [tplArg tpl, TmArg (EVar (ExprVarName (unChoiceName choice))), TmArg cid, TmArg actor, TmArg arg]
    UExerciseByKey tpl choice key arg ->
      pPrintAppKeyword lvl prec "exercise_by_key"
      [tplArg tpl, TmArg (EVar (ExprVarName (unChoiceName choice))), TmArg key, TmArg arg]
    UFetch tpl cid ->
      pPrintAppKeyword lvl prec "fetch" [tplArg tpl, TmArg cid]
    UGetTime ->
      keyword_ "get_time"
    UEmbedExpr typ e ->
      pPrintAppKeyword lvl prec "uembed_expr" [TyArg typ, TmArg e]
    UFetchByKey RetrieveByKey{..} ->
      pPrintAppKeyword lvl prec "ufetch_by_key" [tplArg retrieveByKeyTemplate, TmArg retrieveByKeyKey]
    ULookupByKey RetrieveByKey{..} ->
      pPrintAppKeyword lvl prec "ulookup_by_key" [tplArg retrieveByKeyTemplate, TmArg retrieveByKeyKey]

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

instance Pretty DefTypeSyn where
  pPrintPrec lvl _prec (DefTypeSyn mbLoc syn params typ) =
    withSourceLoc lvl mbLoc $ (keyword_ "synonym" <-> lhsDoc) $$ nest 2 (pPrintPrec lvl 0 typ)
    where
      lhsDoc = pPrint syn <-> hsep (map (pPrintAndKind lvl precParam) params) <-> "="

instance Pretty DefDataType where
  pPrintPrec lvl _prec (DefDataType mbLoc tcon (IsSerializable serializable) params dataCons) =
    withSourceLoc lvl mbLoc $ case dataCons of
    DataRecord fields ->
      hang (keyword_ "record" <-> lhsDoc) 2 (pPrintRecord lvl docHasType fields)
    DataVariant variants ->
      (keyword_ "variant" <-> lhsDoc) $$ nest 2 (vcat (map pPrintVariantCon variants))
    DataEnum enums ->
      (keyword_ "enum" <-> lhsDoc) $$ nest 2 (vcat (map pPrintEnumCon enums))
    where
      lhsDoc =
        serializableDoc <-> pPrint tcon <-> hsep (map (pPrintAndKind lvl precParam) params) <-> "="
      serializableDoc = if serializable then "@serializable" else empty
      pPrintVariantCon (name, typ) =
        "|" <-> pPrint name <-> pPrintPrec lvl precHighest typ
      pPrintEnumCon name = "|" <-> pPrint name

instance Pretty DefValue where
  pPrintPrec lvl _prec (DefValue mbLoc binder (HasNoPartyLiterals noParties) (IsTest isTest) body) =
    withSourceLoc lvl mbLoc $
    vcat
      [ hang (keyword_ kind <-> annot <-> pPrintAndType lvl precBinding binder <-> "=") 2 (pPrintPrec lvl 0 body) ]
    where
      kind = if isTest then "test" else "def"
      annot = if noParties then empty else "@partyliterals"

pPrintTemplateChoice ::
  PrettyLevel -> ModuleName -> TypeConName -> TemplateChoice -> Doc ann
pPrintTemplateChoice lvl modName tpl (TemplateChoice mbLoc name isConsuming controller selfBinder argBinder retType update) =
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
    , nest 2 (keyword_ "controller" <-> pPrintPrec lvl 0 controller)
    , nest 2 (keyword_ "do" <-> pPrintPrec lvl 0 update)
    ]

pPrintTemplate ::
  PrettyLevel -> ModuleName -> Template -> Doc ann
pPrintTemplate lvl modName (Template mbLoc tpl param precond signatories observers agreement choices mbKey) =
  withSourceLoc lvl mbLoc $
    keyword_ "template" <-> pPrint tpl <-> pPrint param
    <-> keyword_ "where"
    $$ nest 2 (vcat ([signatoriesDoc, observersDoc, precondDoc, agreementDoc] ++ mbKeyDoc ++ choiceDocs))
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

pPrintFeatureFlags :: FeatureFlags -> Doc ann
pPrintFeatureFlags flags
  | forbidPartyLiterals flags = empty
  | otherwise = "@allowpartyliterals"

instance Pretty Module where
  pPrintPrec lvl _prec (Module modName _path flags synonyms dataTypes values templates) =
    vcat $
      pPrintFeatureFlags flags
      : (keyword_ "module" <-> pPrint modName <-> keyword_ "where")
      : concat
        [ map (pPrintPrec lvl 0) (NM.toList dataTypes)
        , map (pPrintPrec lvl 0) (NM.toList synonyms)
        , map (pPrintPrec lvl 0) (NM.toList values)
        , map (pPrintTemplate lvl modName) (NM.toList templates)
        ]

instance Pretty PackageName where
    pPrint = pPrint . unPackageName

instance Pretty PackageVersion where
    pPrint = pPrint . unPackageVersion

instance Pretty PackageMetadata where
    pPrint (PackageMetadata name version) = pPrint name <> "-" <> pPrint version

instance Pretty Package where
  pPrintPrec lvl _prec (Package version modules metadata) =
    vcat $
      "daml-lf" <-> pPrintPrec lvl 0 version
      : maybe empty (\m -> "metadata" <-> pPrintPrec lvl 0 m) metadata
      : map (\m -> "" $-$ pPrintPrec lvl 0 m) (NM.toList modules)
