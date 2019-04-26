package com.mongodb.migratecluster;

import org.apache.commons.cli.ParseException;

import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.commandline.ApplicationOptionsLoader;
import com.mongodb.migratecluster.commandline.InputArgsParser;
import com.mongodb.migratecluster.oplog.OplogReader;
import com.mongodb.migratecluster.oplog.OplogWriter;

/**
 * File: Application Author: Shyam Arjarapu Date: 1/12/17 9:40 AM Description:
 *
 * A class to run the migration of MongoDB cluster based on the inputs configured in config file
 *
 */
public class Application {

	public static void main(String[] args) {
		ApplicationOptions options;

		InputArgsParser parser = new InputArgsParser();
		try {
			options = parser.getApplicationOptions(args);
		} catch (ParseException e) {
			parser.printHelp();
			return;
		}

		if (options.isShowHelp()) {
			parser.printHelp();
			return;
		}

		String configFilePath = options.getConfigFilePath();
		if (configFilePath != "") {
			options = ApplicationOptionsLoader.load(configFilePath);
		}

		OplogReader reader = new OplogReader(options);
		new Thread(reader, "Reader").start();

		OplogWriter writer = new OplogWriter(options);
		writer.applyOperations(reader.queue);
	}

}
