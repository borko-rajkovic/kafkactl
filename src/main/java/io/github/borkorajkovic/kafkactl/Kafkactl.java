package io.github.borkorajkovic.kafkactl;

import io.github.borkorajkovic.kafkactl.command.CopyOffsetsCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
        name = "kafkactl",
        mixinStandardHelpOptions = true,
        version = "0.1.0",
        description = "Kafka administration CLI tool",
        subcommands = {CopyOffsetsCommand.class}
)
public class Kafkactl implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Kafkactl.class);

    public static void main(String[] args) {
        log.debug("Starting kafkactl with args: {}", (Object) args);

        int exitCode = new CommandLine(new Kafkactl()).execute(args);

        log.debug("Exit code: {}", exitCode);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        log.info("kafkactl started. Use --help for commands.");
    }
}
