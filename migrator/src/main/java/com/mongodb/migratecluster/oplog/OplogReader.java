package com.mongodb.migratecluster.oplog;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.migratecluster.commandline.ApplicationOptions;

/**
 * File: OplogBufferedReader Author: Shyam Arjarapu Date: 1/14/19 9:50 AM Description:
 *
 * A class to help read the oplog entries and notify the consumers of documents read either in bulk or timer based mode.
 */
public class OplogReader implements Runnable {
	private final int BUFFER_SIZE = 1000000;

	public final ConcurrentLinkedQueue<Document> queue = new ConcurrentLinkedQueue<Document>();

	final static Logger logger = LoggerFactory.getLogger(OplogReader.class);

	private ApplicationOptions options;

	public OplogReader(ApplicationOptions options) {
		this.options = options;

	}

	private BsonTimestamp getLatestOplogTsFromTarget() {
		MongoCollection<Document> collection = options.getTargetClient().getDatabase("local").getCollection("oplog.rs");

		Bson filter = Filters.and(Filters.eq("op", "i"), Filters.not(Filters.regex("ns", ".*system.*")));

		Document doc = collection.find(filter).sort(Sorts.descending("$natural")).limit(1).first();

		logger.info("latest inserted doc: " + doc.toJson());

		return doc.get("ts", BsonTimestamp.class);
	}

	@Override
	public void run() {
		BsonTimestamp ts = getLatestOplogTsFromTarget();

		// 5 minutes earlier
		BsonTimestamp newTs = new BsonTimestamp(ts.getTime() - 5 * 60, 0);

		TimeZone tz = TimeZone.getTimeZone("America/Montreal");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		df.setTimeZone(tz);
		long t = newTs.getTime();
		Date td = new Date(t * 1000);
		logger.info("Guessed start time: " + df.format(td));

		MongoDatabase db = options.getSourceClient().getDatabase("local");
		MongoCollection<Document> collection = db.withReadPreference(ReadPreference.secondary()).getCollection("oplog.rs");

		MongoCursor<Document> cursor = collection.find(Filters.gte("ts", newTs)).cursorType(CursorType.Tailable).noCursorTimeout(true).iterator();

		int count = 0;

		while (true) {

			Document document = cursor.next();
			while (document == null) {
				document = cursor.next();
				logger.info("end of oplog");

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			while (queue.size() >= BUFFER_SIZE) {
				logger.info("queue full");
				Thread.yield();
			}

			queue.add(document);
			count++;

			if (count >= BUFFER_SIZE) {
				count = 0;
				logger.info("queue size: {}", queue.size());
			}

		}
	}

}
