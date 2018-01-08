import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.*;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptChunk;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by stefano on 23/02/17.
 */
public class MongoConnector {
    private MongoClient mongoClient;
    private MongoCollection<Document> transactionCollection;
    private MongoCollection<Document> ratecollection;

    private boolean connected = false;

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    public void connect() {
        if (!connected) {
            mongoClient = new MongoClient(Settings.MONGO_SERVER_IP, Settings.MONGO_SERVER_PORT);

            MongoDatabase db = mongoClient.getDatabase(Settings.MONGO_DB_NAME);
            transactionCollection = db.getCollection(Settings.MONGO_COLLECTION_NAME);
            connected = true;
        }
    }

    public void disconnect() {
        if (connected) {
            mongoClient.close();
            connected = false;
            System.out.println("Disconnect to Mongo");
        }
    }



    /*
     * This method find the last block stored in the database and returns its Locktime
     * @return The time (a long value) of the last block stored in the DB
     */
    protected long getLastBlockTime() {
        //Take the last transaction from the DB
        Document cursor = transactionCollection.find().sort(new BasicDBObject("time", -1)).first();
        if (cursor == null) {
            return 0l;
        }
        return ((long) cursor.get("time"));
    }


    /*
    * This method find all the transactions received by an address
    *
     */
    protected FindIterable<Document> getReceiveTransactions(String address) {
        //System.out.println("Call functiongetReceiveTransactions of an address!");
        Document queryBilancioInIngresso = new Document();
        queryBilancioInIngresso.append("vout.addresses", address);
        queryBilancioInIngresso.append("vin.address", new Document("$ne", address));
        return transactionCollection.find(queryBilancioInIngresso);
    }

    /*
    * This method find all the transactions received by an address and return that transaction sorted by time
    *
    */
    protected FindIterable<Document> getReceiveTransactionsSortedByTime(String address) {
        //System.out.println("Call functiongetReceiveTransactions of an address!");
        Document queryBilancioInIngresso = new Document();
        queryBilancioInIngresso.append("vout.addresses", address);
        queryBilancioInIngresso.append("vin.address", new Document("$ne", address));

        Document sortCondition = new Document();
        sortCondition.append("time", 1);
        return transactionCollection.find(queryBilancioInIngresso).sort(sortCondition);//ordinati
    }

    /*
    * This method find all the sendind transactions of an address
     */
    protected AggregateIterable<Document> getSendTransactions(String address) {
        //System.out.println("Call function getSendTransactions of an address!");
        List<Document> query = new ArrayList<>();
        query.add(Document.parse("{$match : {\"vin.address\" : \"" + address + "\"}}"));
        query.add(Document.parse("{$unwind : \"$vin\"}"));
        query.add(Document.parse("{$match: {\"vin.address\" : \"" + address + "\"}}"));
        query.add(Document.parse("{$lookup:{from: \"transaction\",localField: \"vin.txid\",foreignField: \"txid\", as: \"input\" }}"));
        return transactionCollection.aggregate(query).allowDiskUse(true);
    }

    /*
    * This method find all the sendind transactions of an address and return that transaction sorted by time
    */
    protected AggregateIterable<Document> getSendTransactionsSortedByTime(String address) {
        //System.out.println("Call function getSendTransactions of an address!");
        List<Document> query = new ArrayList<>();
        query.add(Document.parse("{$match : {\"vin.address\" : \"" + address + "\"}}"));
        query.add(Document.parse("{$unwind : \"$vin\"}"));
        query.add(Document.parse("{$match: {\"vin.address\" : \"" + address + "\"}}"));
        query.add(Document.parse("{$lookup:{from: \"transaction\",localField: \"vin.txid\",foreignField: \"txid\", as: \"input\" }}"));
        query.add(Document.parse("{$sort: {\"time\":1}}"));
        return transactionCollection.aggregate(query).allowDiskUse(true);
    }

    protected FindIterable<Document> getAllSendTransactions(String address) {
        //System.out.println("Call function getSendTransactions of an address!");
        org.bson.Document query = new org.bson.Document("vin.address", address);
        return transactionCollection.find(query);
    }

    /*
    * This method find when the address init to receive/send and when the address stop to receive/send
     */
    protected ArrayList<Long> getTotalInitAndEndTimeOfAddress(String address) {
        long initTime = 0;
        long endTime = 0;
        long currentTime = 0;
        ArrayList<Long> initAndEnd = new ArrayList<>();

        ArrayList<Document> orr = new ArrayList<>();
        orr.add(new Document("vout.addresses", address));
        orr.add(new Document("vin.address", address));
        Document query = new Document();
        query.append("$or", orr);

        MongoCursor<Document> cursor = transactionCollection.find(query).iterator();
        initTime = (long) cursor.next().get("time");
        endTime = initTime;
        while (cursor.hasNext()) {
            currentTime = (long) cursor.next().get("time");
            if (currentTime > endTime) {
                endTime = currentTime;
            } else {
                if (currentTime < initTime) {
                    initTime = currentTime;
                }
            }
        }
        cursor.close();
        initAndEnd.add(initTime);
        initAndEnd.add(endTime);
        //System.out.println("InitTime-> " + initTime);
        //System.out.println("EndTime -> " + endTime);
        return initAndEnd;
    }

    /*
    * This method find when the address init to send and when it stop
     */
    protected ArrayList<Long> getSendingInitAndEndTimeOfAddress(String address) {
        long initTime = 0;
        long endTime = 0;
        long currentTime = 0;
        ArrayList<Long> initAndEnd = new ArrayList<>();

        Document query = new Document();
        query.append("vin.address", address);

        MongoCursor<Document> cursor = transactionCollection.find(query).iterator();
        initTime = (long) cursor.next().get("time");
        endTime = initTime;
        while (cursor.hasNext()) {
            currentTime = (long) cursor.next().get("time");
            if (currentTime > endTime) {
                endTime = currentTime;
            } else {
                if (currentTime < initTime) {
                    initTime = currentTime;
                }
            }
        }
        cursor.close();
        initAndEnd.add(initTime);
        initAndEnd.add(endTime);
        //System.out.println("InitTime-> " + initTime);
        //System.out.println("EndTime -> " + endTime);
        return initAndEnd;
    }

    /*
    * This method find when the address init to receive and when it stop
     */
    protected ArrayList<Long> getReceivingInitAndEndTimeOfAddress(String address) {
        long initTime = 0;
        long endTime = 0;
        long currentTime = 0;
        ArrayList<Long> initAndEnd = new ArrayList<>();

        MongoCursor<Document> cursor = getReceiveTransactions(address).iterator();
        initTime = (long) cursor.next().get("time");
        endTime = initTime;


        while (cursor.hasNext()) {
            currentTime = (long) cursor.next().get("time");
            if (currentTime > endTime) {
                endTime = currentTime;
            } else {
                if (currentTime < initTime) {
                    initTime = currentTime;
                }
            }
        }

        cursor.close();

        initAndEnd.add(initTime);
        initAndEnd.add(endTime);
        //System.out.println("InitTime-> " + initTime);
        //System.out.println("EndTime -> " + endTime);
        return initAndEnd;
    }


    /*
    *
    *
     */
    public int getNumberTransactions(String address) {
        ArrayList<Document> orr = new ArrayList<>();
        orr.add(new Document("vout.addresses", address));
        orr.add(new Document("vin.address", address));
        return (int) transactionCollection.count(new Document("$or", orr));
    }

    /*
   *
   *
    */
    public int getNumberTransactionsCluster(ArrayList<String> clusterAddress) {
        ArrayList<Document> orr = new ArrayList<>();
        orr.add(new Document("vout.addresses", new Document("$in", clusterAddress)));
        orr.add(new Document("vin.address", new Document("$in", clusterAddress)));
        return (int) transactionCollection.count(new Document("$or", orr));
    }


    public int getNumberTransactions(ArrayList<Long> send, ArrayList<Long> income) {
        return (send.size() + income.size());
    }


    protected FindIterable<Document> getReceiveTransactionsCluster(ArrayList<String> addresses) {
        //System.out.println("Call functiongetReceiveTransactions of an address!");
        Document queryBilancioInIngresso = new Document();
        queryBilancioInIngresso.append("vout.addresses",
                new Document("$in", addresses));
        queryBilancioInIngresso.append("vin.address",
                new Document("$nin", addresses));
        return transactionCollection.find(queryBilancioInIngresso);
    }

    /*
  * This method find when the clusterAddresses init to receive/send and when the clusterAddresses stop to receive/send
   */
    protected ArrayList<Long> getTotalInitAndEndTimeOfCluster(ArrayList<String> clusterAddresses) {
        long initTime = 0;
        long endTime = 0;
        long currentTime = 0;
        ArrayList<Long> initAndEnd = new ArrayList<>();

        ArrayList<Document> orr = new ArrayList<>();
        orr.add(new Document("vout.addresses", new Document("$in", clusterAddresses)));
        orr.add(new Document("vin.address", new Document("$in", clusterAddresses)));
        Document query = new Document();
        query.append("$or", orr);

        MongoCursor<Document> cursor = transactionCollection.find(query).iterator();
        initTime = (long) cursor.next().get("time");
        endTime = initTime;
        while (cursor.hasNext()) {
            currentTime = (long) cursor.next().get("time");
            if (currentTime > endTime) {
                endTime = currentTime;
            } else {
                if (currentTime < initTime) {
                    initTime = currentTime;
                }
            }
        }
        cursor.close();
        initAndEnd.add(initTime);
        initAndEnd.add(endTime);
        //System.out.println("InitTime-> " + initTime);
        //System.out.println("EndTime -> " + endTime);
        return initAndEnd;
    }

    /*
* This method find all the sendind transactions of the cluster
 */
    protected AggregateIterable<Document> getSendTransactionsCluster(ArrayList<String> clusteraAdresses) {
        //System.out.println("Call function getSendTransactions of an address!");
        String asArray = "";
        for (int i = 0; i < clusteraAdresses.size(); i++) {
            asArray += "\"" + clusteraAdresses.get(i) + "\"";
            if (i < (clusteraAdresses.size() - 1)) asArray += ",";
        }
        List<Document> query = new ArrayList<>();
        query.add(Document.parse("{$match : {\"vin.address\" : {$in :[" + asArray + "]}}}"));
        query.add(Document.parse("{$unwind : \"$vin\"}"));
        query.add(Document.parse("{$match: {\"vin.address\" : {$in :[" + asArray + "]}}}"));
        query.add(Document.parse("{$lookup:{from: \"transaction\",localField: \"vin.txid\",foreignField: \"txid\", as: \"input\" }}"));
        return transactionCollection.aggregate(query).allowDiskUse(true);
    }

}
