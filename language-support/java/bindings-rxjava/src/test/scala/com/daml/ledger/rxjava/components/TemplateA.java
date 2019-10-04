package com.daml.ledger.rxjava.components;

import com.daml.ledger.javaapi.data.*;
import java.util.List;

public class TemplateA extends Template {
  public static final Identifier TEMPLATE_ID =
      new Identifier("SomePackage", "SomeModule", "TemplateA");

  public final String argument;

  public TemplateA(String argument) {
    this.argument = argument;
  }

  @Override
  public CreateCommand create() {
    throw new IllegalStateException("unreachable code");
  }

  public static TemplateA fromValue(Value value$) throws IllegalArgumentException {
    Record record$ = value$.asRecord().get();
    List<Record.Field> fields$ = record$.getFields();
    String argument = fields$.get(0).getValue().asText().get().getValue();
    return new TemplateA(argument);
  }
}
