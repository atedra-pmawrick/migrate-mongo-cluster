package com.mongodb.migratecluster.oplog;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.migratecluster.commandline.ApplicationOptions;
import com.mongodb.migratecluster.model.Resource;
import com.mongodb.migratecluster.predicates.CollectionFilterPredicate;
import com.mongodb.migratecluster.predicates.DatabaseFilterPredicate;

/**
 * File: OplogWriter Author: Shyam Arjarapu Date: 1/14/19 7:20 AM Description:
 *
 * A class to help write the apply the oplog entries on the target
 */
public class OplogWriter {
	private final HashMap<String, Boolean> allowedNamespaces = new HashMap<>();

	private final MongoClient targetClient;
	private MongoClient sourceClient;

	private final static Logger logger = LoggerFactory.getLogger(OplogWriter.class);
	private final DatabaseFilterPredicate databasePredicate;
	private final CollectionFilterPredicate collectionPredicate;

	private final int BATCH_SIZE = 1000;

	private LocalDateTime last = LocalDateTime.now();

	public OplogWriter(ApplicationOptions options) {
		targetClient = options.getTargetClient();
		sourceClient = options.getSourceClient();

		databasePredicate = new DatabaseFilterPredicate(options.getBlackListFilter());
		collectionPredicate = new CollectionFilterPredicate(options.getBlackListFilter());
	}

	/**
	 * Applies the oplog documents on the oplog store
	 *
	 * @param operations
	 *          a list of oplog operation documents
	 */
	public void applyOperations(ConcurrentLinkedQueue<Document> queue) {
		Map<String, List<WriteModel<Document>>> namespaceModels = new HashMap<String, List<WriteModel<Document>>>();

		while (true) {
			Document doc = queue.poll();

			while (doc == null) {
				logger.info(String.format("queue empty"));

				namespaceModels.forEach((ns, models) -> {
					if (models.size() > 0) {
						applyBulkWriteModelsOnCollection(ns, models);
						logger.info("draining buffer {} for {} docs", ns, models.size());
						models.clear();
					}
				});

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				doc = queue.poll();
			}

			logGapStats(doc);

			String ns = doc.getString("ns");
			if (!isNamespaceAllowed(ns)) {
				continue;
			}

			List<WriteModel<Document>> models = namespaceModels.get(ns);
			if (models == null) {
				models = new ArrayList<WriteModel<Document>>(BATCH_SIZE);
				namespaceModels.put(ns, models);
			}

			WriteModel<Document> model = getWriteModelForOperation(doc);
			if (model != null) {
				models.add(model);
			}

			if (models.size() == BATCH_SIZE) {
				applyBulkWriteModelsOnCollection(ns, models);
				models.clear();
			}

		}
	}

	private Document getLatestOplogEntryFromSource() {
		MongoCollection<Document> collection = sourceClient.getDatabase("local").getCollection("oplog.rs");
		return collection.find().sort(Sorts.descending("$natural")).limit(1).first();
	}

	private boolean isNamespaceAllowed(String namespace) {
		if (!allowedNamespaces.containsKey(namespace)) {
			boolean allow = checkIfNamespaceIsAllowed(namespace);
			allowedNamespaces.put(namespace, allow);
		}
		// return cached value
		return allowedNamespaces.get(namespace);
	}

	private boolean checkIfNamespaceIsAllowed(String namespace) {
		String databaseName = namespace.split("\\.")[0];
		try {
			Document dbDocument = new Document("name", databaseName);
			boolean isNotBlacklistedDB = databasePredicate.test(dbDocument);
			if (isNotBlacklistedDB) {
				// check for collection as well
				String collectionName = namespace.substring(databaseName.length() + 1);
				Resource resource = new Resource(databaseName, collectionName);
				return collectionPredicate.test(resource);
			} else {
				return false;
			}
		} catch (Exception e) {
			logger.error("error while testing the namespace is in black list or not");
			return false;
		}
	}

	private BulkWriteResult applyBulkWriteModelsOnCollection(String namespace, List<WriteModel<Document>> operations) {

		MongoCollection<Document> collection = getCollectionByNamespace(this.targetClient, namespace);
		try {
			List<WriteModel<Document>> bulkOp = new ArrayList<WriteModel<Document>>();
			for (WriteModel<Document> op : operations) {
				bulkOp.add(op);
			}

			BulkWriteOptions options = new BulkWriteOptions();
			options.ordered(true);

			return collection.bulkWrite(bulkOp, options);

		} catch (MongoBulkWriteException err) {

			for (WriteModel<Document> op : operations) {
				try {
					List<WriteModel<Document>> bulkOp = new ArrayList<WriteModel<Document>>();
					bulkOp.add(op);
					collection.bulkWrite(bulkOp);
				} catch (MongoBulkWriteException err2) {
					for (BulkWriteError bulkWriteError : err2.getWriteErrors()) {
						if (bulkWriteError.getCode() != 11000) {
							logger.warn(bulkWriteError.getMessage() + " " + bulkWriteError.getDetails());
						}
					}
				}
			}

		} catch (Exception e) {
			logger.error("Unplanned", e);
		}
		return null;
	}

	private void logGapStats(Document lastOplogProcessed) {
		LocalDateTime now = LocalDateTime.now();

		Duration diff = Duration.between(last, now);
		if (diff.getSeconds() > 5) {

			Document latestOplogEntryFromSource = getLatestOplogEntryFromSource();

			BsonTimestamp sourceOpTime = latestOplogEntryFromSource.get("ts", BsonTimestamp.class);
			BsonTimestamp targetOpTime = lastOplogProcessed.get("ts", BsonTimestamp.class);

			int gapInSeconds = sourceOpTime.getTime() - targetOpTime.getTime();

			long s = sourceOpTime.getTime();
			Date sd = new Date(s * 1000);
			long t = targetOpTime.getTime();
			Date td = new Date(t * 1000);

			TimeZone tz = TimeZone.getTimeZone("America/Montreal");
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
			df.setTimeZone(tz);

			String message = String.format("Target is behind by %d seconds\nSource: %s\nTarget: %s", gapInSeconds, df.format(sd), df.format(td));
			logger.info(message);

			last = now;
		}

	}

	/**
	 * Get's a WriteModel for the given oplog operation
	 *
	 * @param operation
	 *          an oplog operation
	 * @return a WriteModel of a bulk operation
	 */
	private WriteModel<Document> getWriteModelForOperation(Document operation) {
		String message;
		WriteModel<Document> model = null;
		switch (operation.getString("op")) {
		case "i":
			model = getInsertWriteModel(operation);
			break;
		case "u":
			model = getUpdateWriteModel(operation);
			break;
		case "d":
			model = getDeleteWriteModel(operation);
			break;
		case "c":
			// might have to be individual operation
			performRunCommand(operation);
			break;
		case "n":
			break;
		default:
			message = String.format("unsupported operation %s; op: %s", operation.getString("op"), operation.toJson());
			logger.error(message);
			throw new RuntimeException(message);
		}
		return model;
	}

	private WriteModel<Document> getInsertWriteModel(Document operation) {
		Document document = operation.get("o", Document.class);
		return new InsertOneModel<>(document);
	}

	private WriteModel<Document> getUpdateWriteModel(Document operation) {
		Document find = operation.get("o2", Document.class);
		Document update = operation.get("o", Document.class);
		update.remove("$v");

		if (!update.containsKey("$set")) {
			Document doc = new Document();
			doc.append("$set", update);
			update = doc;
		}

		return new UpdateOneModel<>(find, update);
	}

	private WriteModel<Document> getDeleteWriteModel(Document operation) {
		Document find = operation.get("o", Document.class);
		return new DeleteOneModel<>(find);
	}

	private void performRunCommand(Document operation) {
		Document document = operation.get("o", Document.class);
		String databaseName = operation.getString("ns").replace(".$cmd", "");
		this.targetClient.getDatabase(databaseName).runCommand(document);

		String message = String.format("completed runCommand op on database: %s; document: %s", databaseName, operation.toJson());
		logger.debug(message);
	}

	private MongoCollection<Document> getCollectionByNamespace(MongoClient client, String ns) {
		String[] parts = ns.split("\\.");
		String databaseName = parts[0];
		String collectionName = ns.substring(databaseName.length() + 1);

		return client.getDatabase(databaseName).getCollection(collectionName);
	}

}
