package com.mongodb.migratecluster.commandline;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * File: InputArgsParser Author: shyam.arjarapu Date: 4/13/17 11:47 PM Description:
 */
public class InputArgsParser {
	private Options options;

	public InputArgsParser() {
		options = new Options();
		options.addOption("h", "help", false, "print this message");
		options.addOption("c", "config", true, "configuration file for migration");
	}

	public void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("migratecluster", options, true);
	}

	public ApplicationOptions getApplicationOptions(String[] args) throws ParseException {
		ApplicationOptions appOptions = new ApplicationOptions();

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(this.options, args);

		if (cmd.hasOption("help")) {
			appOptions.setShowHelp(true);
		}
		if (cmd.hasOption("config")) {
			appOptions.setConfigFilePath(cmd.getOptionValue("c", ""));
		}

		return appOptions;
	}
}
