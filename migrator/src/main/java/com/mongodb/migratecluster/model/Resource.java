package com.mongodb.migratecluster.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * File: Resource Author: Shyam Arjarapu Date: 1/11/19 9:30 PM Description:
 *
 * a class representing a resource currently being processed. it could be a collection inside a database
 */
public class Resource {
	private String database;
	private String collection;

	public Resource() {
	}

	public Resource(String database, String collection) {
		this.database = database;
		this.collection = collection;
	}

	/**
	 * Get's the name of the database
	 *
	 * @return a string representing the database
	 */
	@JsonProperty("database")
	public String getDatabase() {
		return database;
	}

	/**
	 * Get's the name of the collection
	 *
	 * @return a string representing the collection
	 */
	@JsonProperty("collection")
	public String getCollection() {
		return collection;
	}

	/**
	 * Get's the namespace of the resource
	 *
	 * @return a string of the namespace
	 */
	@JsonIgnore
	public String getNamespace() {
		if (this.isEntireDatabase()) {
			return this.getDatabase();
		}
		return String.format("%s.%s", this.getDatabase(), this.getCollection());
	}

	/**
	 * Indicates of the Resource represents entire database
	 *
	 * @return a boolean representing if the resource represents entire database or not
	 */
	@JsonIgnore
	public boolean isEntireDatabase() {
		return (this.collection.equals("{}"));
	}

	/**
	 * @return a string representation of the Resource object
	 */
	@Override
	public String toString() {
		return String.format("{ database: \"%s\", collection: \"%s\" }", this.getDatabase(), this.getCollection());
	}
}
