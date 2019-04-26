package com.mongodb.migratecluster.commandline;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * File: ApplicationOptionsLoader Author: shyam.arjarapu Date: 4/13/17 11:47 PM Description:
 */
public class ApplicationOptionsLoader {

	public static ApplicationOptions load(String configFilePath) {
		ObjectMapper mapper = new ObjectMapper(); // create once, reuse
		ApplicationOptions appOptions;
		try {

			File file = new File(configFilePath);
			if (file.exists()) {
				appOptions = mapper.readValue(file, ApplicationOptions.class);
			} else {
				String message = String.format("configFilePath: '%s' does not exists", configFilePath);
				throw new RuntimeException(message);
			}

		} catch (IOException e) {
			String message = String.format("error while reading configFilePath: '%s'. exception: %s", configFilePath, e.getMessage());
			throw new RuntimeException(message, e);
		}
		appOptions.setConfigFilePath(configFilePath);
		return appOptions;
	}
}
