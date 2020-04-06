// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.archive

import com.daml.lf.archive.Decode
import com.daml.lf.archive.testing.Encode
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageMajorVersion.V1
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.testing.parser.{AstRewriter, ParserParameters, parseModules}
import com.daml.lf.validation.Validation
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.language.implicitConversions

class EncodeV1Spec extends WordSpec with Matchers with TableDrivenPropertyChecks {

  import EncodeV1Spec._

  val defaultParserParameters: ParserParameters[this.type] =
    ParserParameters(
      pkgId,
      LanguageVersion(V1, "8")
    )

  "Encode and Decode" should {
    "form a prism" in {

      implicit val defaultParserParameters2: ParserParameters[this.type] =
        defaultParserParameters

      val pkg: Ast.Package =
        p"""

         metadata ( 'foobar' : '0.0.1' )

         module Mod {

            record @serializable Person = { person: Party, name: Text } ;

            template (this : Person) =  {
              precondition True,
              signatories Cons @Party [Mod:Person {person} this] (Nil @Party),
              observers Cons @Party [Mod:Person {person} this] (Nil @Party),
              agreement "Agreement",
              choices {
                choice Sleep (self) (u: Unit) : Unit by Cons @Party [Mod:Person {person} this] (Nil @Party) to upure @Unit (),
                choice @nonConsuming Nap (self) (i : Int64): Int64 by Cons @Party [Mod:Person {person} this] (Nil @Party) to upure @Int64 i
              },
              key @Party (Mod:Person {person} this) (\ (p: Party) -> Cons @Party [p] (Nil @Party))
            };

           variant Tree (a : * ) = Leaf : Unit | Node : Mod:Tree.Node a ;
           record Tree.Node (a: *) = { value: a, left : Mod:Tree a, right : Mod:Tree a };
           enum Color = Red | Green | Blue;

           val aVar: forall (a:*). a -> a = /\ (a: *). \ (x: a) -> x;
           val aValue: forall (a:*). a -> a = Mod:aVar;
           val aBuiltin : Int64 -> Int64 -> Int64 = ADD_INT64;
           val unit: Unit = ();
           val myFalse: Bool = False;
           val myTrue: Bool = True;
           val aInt: Int64 = 14;
           val aDecimal: Numeric 10 = 2.2000000000;
           val aDate: Date = 1879-03-14;
           val aTimestamp: Timestamp = 1970-01-01T00:00:00.000001Z;
           val aParty: Party = 'party';
           val aString: Text = "a string";
           val aStruct: forall (a:*) (b:*). a ->  b -> < x1: a, x2: b > = /\ (a:*) (b:*). \ (x1: a) (x2: b) ->
             <x1 = x1, x2 = x2>;
           val aStructProj: forall (a:*) (b:*). < x1: a, x2: b > -> a = /\ (a:*) (b:*). \ (struct: < x1: a, x2: b >) ->
             (struct).x1;
           val aStructUpd: forall (a:*) (b:*). < x1: a, x2: b > -> a -> < x1: a, x2: b >  =
             /\ (a:*) (b:*). \ (struct: < x1: a, x2: b >) (x: a) ->
               < struct with x1 = x >;
           val aApp: forall (a: *) (b: *) (c:*). (a -> b -> c) -> a -> b -> c =
             /\ (a:*) (b:*) (c: *). \ (f: a -> b -> c) (x: a) (y: b) -> f x y;
           val anEmptyList: forall (a: *). List a = /\ (a: *).
              Nil @a;
           val aNonEmptList: forall (a: *). a -> a -> a -> List a = /\ (a: *). \ (x1:a) (x2: a) (x3: a) ->
             Cons @a [x1, x2, x3] (Nil @a);
           val anEmptyOption: forall (a: *). Option a = /\ (a:*).
             None @a;
           val aNonEmptyOption: forall (a: *). a -> Option a = /\ (a:*). \ (x: a) ->
             Some @a x;
           val aLeaf: forall (a: *). Mod:Tree a = /\ (a:*).
             Mod:Tree:Leaf @a ();
           val aNode: forall (a: *). a -> Mod:Tree a = /\ (a:*). \ (x: a) ->
             Mod:Tree:Node @a (Mod:Tree.Node @a { value = x, left = Mod:Tree:Leaf @a (), right = Mod:Tree:Leaf @a ()});
           val red: Mod:Color =
             Mod:Color:Red;
           val aRecProj: forall (a: *). Mod:Tree.Node a -> a = /\ (a:*). \ (node: Mod:Tree.Node a) ->
             Mod:Tree.Node @a { value } node;
           val aRecUpdate: forall (a: *). Mod:Tree.Node a -> Mod:Tree.Node a = /\ (a:*). \ (node: Mod:Tree.Node a) ->
             Mod:Tree.Node @a { node with left = Mod:Tree:Leaf @a () };
           val aUnitMatch: forall (a: *). a -> Unit -> a = /\ (a: *). \(x :a ) (e:Unit) ->
             case e of () -> x;
           val aBoolMatch: forall (a: *). a -> a -> Bool -> a = /\ (a: *). \(x :a) (y: a) (e: Bool) ->
             case e of True -> x | False -> y;
           val aListMatch: forall (a: *). List a -> Option (<head: a, tail: List a>) = /\ (a: *). \ (e: List a) ->
             case e of Nil -> None @(<head: a, tail: List a>)
                     | Cons h t -> Some @(<head: a, tail: List a>) (<head = h, tail = t>);
           val aOptionMatch: forall (a: *). Text -> TextMap a -> a -> a = /\ (a:*). \ (key: Text) (map: TextMap a) (default: a) ->
             case (TEXTMAP_LOOKUP @a key map) of None -> default | Some y -> y;
           val aVariantMatch: forall (a:*). Mod:Tree a -> Option a = /\ (a: *). \(e: Mod:Tree a) ->
             case e of Mod:Tree:Leaf x -> None @a
                     | Mod:Tree:Node node -> Some @a (Mod:Tree.Node @a { value } node);
           val aEnumMatch: Mod:Color -> Text = \(e: Mod:Color) ->
             case e of Mod:Color:Red -> "Red" | Mod:Color:Green -> "Green" | Mod:Color:Blue -> "Blue";
           val aLet: Int64 = let i: Int64 = 42 in i;

           val aPureUpdate: forall (a: *). a -> Update a = /\ (a: *). \(x: a) ->
             upure @a x;
           val anUpdateBlock: forall (a: *). a -> Update a = /\ (a: *). \(x: a) ->
             ubind y: a <- (Mod:aPureUpdate @a x) in upure @a y;
           val aCreate: Mod:Person -> Update (ContractId Mod:Person) = \(person: Mod:Person) ->
             create @Mod:Person person;
           val anExercise: (ContractId Mod:Person) -> Update Unit = \(cId: ContractId Mod:Person) ->
             exercise @Mod:Person Sleep cId ();
           val anExerciseWithActor: (ContractId Mod:Person) -> List Party -> Update Int64 =
             \(cId: ContractId Mod:Person) (parties: List Party) ->
                exercise_with_actors @Mod:Person Nap cId parties 1;
           val aFecthByKey: Party -> Update <contract: Mod:Person, contractId: ContractId Mod:Person> = \(party: Party) ->
             fetch_by_key @Mod:Person party;
           val aLookUpByKey: Party -> Update (Option (ContractId Mod:Person)) = \(party: Party) ->
             lookup_by_key @Mod:Person party;
           val aGetTime: Update Timestamp =
             uget_time;
           val anEmbedExpr: forall (a: *). Update a -> Update a = /\ (a: *). \ (x: Update a) ->
             uembed_expr @a x;
           val isZero: Int64 -> Bool = EQUAL @Int64 0;
         }
        
      """

      validate(pkgId, pkg)

      val archive = Encode.encodeArchive(pkgId -> pkg, defaultParserParameters.languageVersion)
      val ((hashCode @ _, decodedPackage: Package), _) = Decode.readArchiveAndVersion(archive)

      pkg shouldBe normalize(decodedPackage, hashCode, pkgId)
    }

    "Encoding of function type with different versions should work as expected" in {

      val text =
        """
        module Mod{
        
          val f : forall (a:*) (b: *) (c: *). a -> b -> c -> Unit =
            /\  (a:*) (b: *) (c: *). \ (xa: a) (xb: b) (xc: c) -> ();
        }
     """
      val versions =
        Table(
          "minVersion",
          LanguageVersion(V1, "0"),
          LanguageVersion(V1, "1"),
          LanguageVersion.default)

      forEvery(versions) { version =>
        implicit val parserParameters: ParserParameters[version.type] =
          ParserParameters(pkgId, version)

        val metadata =
          if (LanguageVersion.ordering.gteq(version, LanguageVersion.Features.packageMetadata)) {
            Some(
              PackageMetadata(
                PackageName.assertFromString("encodespec"),
                PackageVersion.assertFromString("1.0.0")))
          } else None
        val pkg = Package(parseModules(text).right.get, Set.empty, metadata)
        val archive = Encode.encodeArchive(pkgId -> pkg, version)
        val ((hashCode @ _, decodedPackage: Package), _) = Decode.readArchiveAndVersion(archive)

        pkg shouldBe normalize(decodedPackage, hashCode, pkgId)
      }

    }
  }

}

object EncodeV1Spec {

  private implicit def toPackageId(s: String): PackageId = PackageId.assertFromString(s)

  private val pkgId: PackageId = "self"

  private def normalize(pkg: Package, hashCode: PackageId, selfPackageId: PackageId): Package = {

    val replacePkId: PartialFunction[Identifier, Identifier] = {
      case Identifier(`hashCode`, name) => Identifier(selfPackageId, name)
    }
    lazy val dropEAbsRef: PartialFunction[Expr, Expr] = {
      case EAbs(binder, body, Some(_)) =>
        EAbs(normalizer.apply(binder), normalizer.apply(body), None)
    }
    lazy val normalizer = new AstRewriter(exprRule = dropEAbsRef, identifierRule = replacePkId)

    normalizer.apply(pkg)
  }

  private def validate(pkgId: PackageId, pkg: Package): Unit =
    Validation
      .checkPackage(Map(pkgId -> pkg), pkgId)
      .left
      .foreach(e => sys.error(e.toString))

}
