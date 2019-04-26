package com.mongodb.migratecluster.commandline;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * File: ApplicationOptions Author: shyam.arjarapu Date: 4/13/17 11:47 PM Description:
 */
public class ApplicationOptions {
	private String sourceCluster;
	private String targetCluster;
	private String configFilePath;
	private boolean showHelp;
	private List<ResourceFilter> blackListFilter;

	private MongoClient sourceClient;
	private MongoClient targetClient;

	public ApplicationOptions() {
		sourceCluster = "";
		targetCluster = "";
		configFilePath = "";
		showHelp = false;
		setBlackListFilter(new ArrayList<>());
	}

	@JsonProperty("sourceCluster")
	public String getSourceCluster() {
		return sourceCluster;
	}

	public void setSourceCluster(String sourceCluster) {
		this.sourceCluster = sourceCluster;
	}

	@JsonProperty("targetCluster")
	public String getTargetCluster() {
		return targetCluster;
	}

	public void setTargetCluster(String targetCluster) {
		this.targetCluster = targetCluster;
	}

	@JsonProperty("configFilePath")
	public String getConfigFilePath() {
		return configFilePath;
	}

	public void setConfigFilePath(String configFilePath) {
		this.configFilePath = configFilePath;
	}

	public boolean isShowHelp() {
		return showHelp;
	}

	public void setShowHelp(boolean showHelp) {
		this.showHelp = showHelp;
	}

	@JsonProperty("blackListFilter")
	public List<ResourceFilter> getBlackListFilter() {
		return blackListFilter;
	}

	public void setBlackListFilter(List<ResourceFilter> blackListFilter) {
		this.blackListFilter = blackListFilter;
	}

	/**
	 * Get's the Mongo Client pointing to the custom cluster
	 *
	 * @param cluster
	 *          a string representing a mongodb servers
	 * @return a MongoClient object pointing to specific cluster
	 */
	private MongoClient getMongoClient(String cluster) {
		String connectionString = String.format("mongodb://%s", cluster);
		MongoClientURI uri = new MongoClientURI(connectionString);
		return new MongoClient(uri);
	}

	/**
	 * Get's the Mongo Client pointing to the source cluster
	 *
	 * @return a MongoClient object pointing to source
	 */
	public MongoClient getSourceClient() {
		if (sourceClient == null) {
			sourceClient = getMongoClient(getSourceCluster());
		}
		return sourceClient;
	}

	/**
	 * Get's the Mongo Client pointing to the target cluster
	 *
	 * @return a MongoClient object pointing to target
	 */
	public MongoClient getTargetClient() {
		if (targetClient == null) {
			targetClient = getMongoClient(getTargetCluster());
		}
		return targetClient;
	}

}
