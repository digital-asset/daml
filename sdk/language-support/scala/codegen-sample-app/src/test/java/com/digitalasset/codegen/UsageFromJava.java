// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen;

import com.daml.ledger.api.v1.event.CreatedEvent;
import com.daml.ledger.api.v1.value.Value;
import com.daml.ledger.client.binding.Contract;
import com.daml.ledger.client.binding.DomainCommand;
import com.daml.ledger.client.binding.EventDecoderApi;
import com.daml.ledger.client.binding.Template;
import com.daml.ledger.client.binding.TemplateCompanion;
import com.daml.sample.EventDecoder$;
import com.daml.sample.MyMain.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import scala.collection.immutable.Seq;

/** Using Daml Scala codegen classes from Java. */
final class UsageFromJava {
  private UsageFromJava() {}

  static <A, B, C> PolyRec<A, B, C> buildingARecord(A a, B b, C c) {
    PolyRec<A, B, C> oneChoice = new PolyRec<>(a, b, c);
    return PolyRec.apply(a, b, c); // preferred, future-safe form
  }

  static Option<String, String> buildingAVariant(String call) {
    return new Option.Call<>(call);
  }

  static void lookingUpFields(MyRecord r) {
    // getter methods have the names of the record fields
    long a = r.a(); // Int64
    BigDecimal b = r.b().bigDecimal(); // Decimal
    // TODO SC DEL-5746 make a translucent newtype (like `e` below) so
    // Object becomes String (1 point task)
    Object c = r.c(); // Party
    String d = r.d(); // Text
    Instant e = r.e(); // Timestamp or Datetime
    r.f(); // unit is void
    boolean g = r.g(); // boolean
    // TODO SC DEL-5746 coercion (i.e. subst, *not* conversion) between
    // scala.Long and java.lang.Long, or a different signature so Scala
    // substitutes something different for the type argument here. Only
    // relevant for long and boolean. (2 point task)
    List<? extends Object> h = seqToList(r.h());
    // other types erase as expected
  }

  static <T> T matchingVariants(Option<T, T> opt) {
    if (opt instanceof Option.Call) return ((Option.Call<T, T>) opt).body();
    else if (opt instanceof Option.Put) return ((Option.Put<T, T>) opt).body();
    else throw new RuntimeException("yeah ok sure");
  }

  // depending on DEL-5482 for non-scalapb, but looks just like this
  static DomainCommand createSomeContract() {
    return makeCreateCommand(CallablePayout.apply("alice", "bob"));
  }

  // likewise, DEL-5482, but looks like this
  static CallablePayout parseTemplateInstance(Value v) {
    // TODO and you would probably want aliases for MODULE$ (1 point task)
    return parseTemplate(CallablePayout$.MODULE$, v);
  }

  // not right for the single-contract case, but good if you're
  // not sure which contract you're holding
  static CallablePayout parseContractThenCast(CreatedEvent ev) {
    return (CallablePayout) parseAnyContractFromModule(EventDecoder$.MODULE$, ev).value();
  }

  // in the present code the choice exercise functions are awkward to access
  static DomainCommand invokeTransfer1(
      Contract<CallablePayout> cp, String controller, String toParty) {
    return new CallablePayout.CallablePayout$u0020syntax(cp.contractId())
        .exerciseTransfer(controller, toParty);
  }

  // I suggest a new parent type for the choice record types, so
  // this can be written instead (see exerciseChoice below)
  // (1 point task)
  /*
  static DomainCommand invokeTransfer2(Contract<CallablePayout> cp, String toParty) {
      return exerciseChoice(contractId(cp), new CallablePayout.Transfer(toParty));
  }
  */

  /////////
  // some utilities to define in Scala, not as part of applications;
  // simple placeholder (but working) definitions below
  /////////

  // DEL-5482 enables the definition of a version of this utility
  // *in Scala* that returns a different DomainCommand
  private static <T> DomainCommand makeCreateCommand(Template<T> tpl) {
    return tpl.create(scala.Predef.DummyImplicit$.MODULE$.dummyImplicit());
  }

  // likewise, DEL-5482 enables the definition of this utility
  // accepting a non-scalapb Value, and a related parseContract.
  // Again, the production definition should be *in Scala*.
  private static <T> T parseTemplate(TemplateCompanion<? extends T> tc, Value v) {
    return tc.the$u0020template$u0020Value()
        .read(v.sum())
        .getOrElse(
            () -> {
              throw new RuntimeException("not a " + tc.id());
            });
  }

  // and likewise DEL-5482
  private static Contract<?> parseAnyContractFromModule(EventDecoderApi ed, CreatedEvent v) {
    return ed.createdEventToContractRef(v)
        .toOption()
        .getOrElse(
            () -> {
              throw new RuntimeException("contract not part of this module");
            });
  }

  private static <T> List<? extends T> seqToList(Seq<? extends T> s) {
    // s.asJava expanded
    return scala.collection.JavaConverters.seqAsJavaList(s);
  }

  @SuppressWarnings("unchecked")
  private static <T> Seq<T> listToSeq(List<? extends T> l) {
    // l.asScala expanded and accounting for Java's not understanding covariance
    // (the Scala code does not need the cast)
    return (Seq<T>) scala.collection.JavaConverters.asScalaBuffer(l);
  }

  /* see invokeTransfer2 above
  private <T> static DomainCommand exerciseChoice(ContractId<? extends T> c, String controller,
                                                  ChoiceOf<? extends T> choice) {
      // ...
  }

  private static <T> ContractId<T> contractId(Contract<T> ct) {
      return new ContractId<>(ct.contractId());
  }
  */

  // another utility
  static <T extends Template<T>> Contract<T> contract(String id, T tpl) {
    return Contract.apply(id, tpl);
  }
}
