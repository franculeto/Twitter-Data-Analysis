package com.twitterfeed.DB;

/**
 * Created by franco on 12/05/17.
 */

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import scala.Tuple2;

import java.net.UnknownHostException;

public class MongoDB {
    public static void main(String args[]) {


        try {
            MongoClient mongoClient = new MongoClient("localhost", 27017);

            DB db = mongoClient.getDB("MongoDB");
            System.out.println("Connect to database successfully at " + db.getName());
            DBCollection coll = db.getCollection("WordsCount");

            BasicDBObject doc = pushDocument(coll);
            coll.insert(doc);

            DBCursor cursor = coll.find();

            while (cursor.hasNext()) {
                DBObject updateDocument = cursor.next();
                System.out.println(cursor.next());
                updateDocument.put("count","200");
                //coll.update();
            }


        } catch (UnknownHostException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }


    }

    private static BasicDBObject pushDocument(DBCollection coll) {
        BasicDBObject doc = new BasicDBObject("word", "Nico").
                append("count", 2);
        return doc;

    }
}