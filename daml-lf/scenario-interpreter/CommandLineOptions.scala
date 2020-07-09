package foo;

import org.scalatest._


class CommandLineOptions extends FlatSpec {

    "CommandLineOptions#showHelp" should """not crash""" in {
        new org.openjdk.jmh.runner.options.CommandLineOptions().showHelp
    }

}
