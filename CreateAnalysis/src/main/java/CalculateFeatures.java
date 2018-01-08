import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Sergio Serusi on 23/05/2017.
 */

public class CalculateFeatures {

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        return map.entrySet()
                .stream()
                //.sorted(Map.Entry.comparingByValue())
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    /**
     * @param mongo
     * @param address
     * @return inTx, inBTC, inUSD, Paying, [FirstDate, LastDate]
     */
    Quintuple<Integer,Long,Double,Integer,ArrayList<Long>> getIncome(MongoConnector mongo, String address) {
        FindIterable<Document> iterable = mongo.getReceiveTransactions(address);
        double totalIncomeUSD = 0;
        long totalebtc = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;

        Set<String> addresses = new HashSet<>();

        int differentAddress = 0;


        long currentTime;
        long receiveValue = 0;
        int i = 0;

        ArrayList<Long> arrayListIncomeValue = new ArrayList<>();


        try (MongoCursor<Document> cursor = iterable.iterator()) {
            while (cursor.hasNext()) {
                try {
                    Document transaction = cursor.next();
                    String txid = (String) transaction.get("txid");
                    //System.out.println(transaction);
                    currentTime = (long) transaction.get("time");
                    long currentValue = 0;

                    for (Document dcVout : ((ArrayList<Document>) transaction.get("vout"))) {
                        for (String dcAddress : ((ArrayList<String>) dcVout.get("addresses"))) {
                            if (dcAddress.equals(address)) {
                                currentValue += (long) dcVout.get("value");
                                //System.out.println("GIorno ->"+currentDay );
                            }
                        }
                    }
                    receiveValue += currentValue;
                    arrayListIncomeValue.add(currentValue);
                    //double exch = mongo2.getRates(txid);
                    double rate = (double)transaction.get("rate");

                    //System.out.println(exch);
                    totalIncomeUSD += (rate * ((float) currentValue / 100000000));

                    if(currentTime<minTime)minTime=currentTime;
                    if(currentTime>maxTime)maxTime=currentTime;

                    ArrayList<String> addressInTransaction = new ArrayList<>();

                    for (Document dcVin : ((ArrayList<Document>) transaction.get("vin"))) {
                        String addr = (String)dcVin.get("address");
                        //System.out.println("Addr: "+addr);
                        if (!addr.equals(address)) {
                            addressInTransaction.add(addr);
                        }
                    }

                    boolean contains= false;
                    for(String st: addressInTransaction){
                        if(addresses.contains(st)){
                            contains = true;
                            break;
                        }
                    }

                    if(contains){
                        addresses.addAll(addressInTransaction);
                    }else{
                        addresses.addAll(addressInTransaction);
                        differentAddress++;
                    }

                } catch (NoSuchElementException e) {
                    System.out.println("Problema ->" + e);
                }

            }
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println("     NUmberInput: " + arrayListIncomeValue.size());
        System.out.println("     RicevutoUSD: " + totalIncomeUSD);
        System.out.println("     RicevutoBTC: " + receiveValue);
        System.out.println("     Paying: "      + differentAddress);

        ArrayList<Long> time = new ArrayList<>();
        time.add(minTime);
        time.add(maxTime);

        return new Quintuple<>(arrayListIncomeValue.size(), receiveValue, totalIncomeUSD, differentAddress, time);
    }

    /**
     * @param mongo
     * @param address
     * @return outTx, outBTC, outUSD, Paid, [FirstDate, LastDate]
     */
    Quintuple<Integer,Long,Double,Integer,ArrayList<Long>> getSpending(MongoConnector mongo, String address) {
        AggregateIterable<Document> iterable = mongo.getSendTransactions(address);
        double totalSendUSD = 0;
        long currentTime = 0l;
        long sendValue = 0l;
        int i = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        ArrayList<Long> arrayListSending = new ArrayList<>();


        String prevTxid = "";
        long sendValueTransaction = 0;
        long totalSum = 0;
        int differentAddress=0;
        Set<String> addresses = new HashSet<>();


        try (MongoCursor<Document> cursor = iterable.iterator()) {
            while (cursor.hasNext()) {

                try {
                    Document transaction = cursor.next();
                    currentTime = (long) transaction.get("time");

                    long currentValue = 0;


                    String currentTxid = (String) transaction.get("txid");
                    //System.out.println(transaction.get("txid"));
                    ArrayList<Document> vout = (ArrayList<Document>) transaction.get("vout");
                    Document vin = (Document) transaction.get("vin");
                    //System.out.println(vin.get("vout"));
                    int index = new Integer(vin.get("vout").toString());
                    //System.out.println("Transa= "+transaction);
                    Document input = ((ArrayList<Document>) transaction.get("input")).get(0);
                    //System.out.println("Input= "+input);

                    ArrayList<Document> voutInput = (ArrayList<Document>) input.get("vout");
                    Document documentInput = voutInput.get(index);
                    long inputValue = (long) documentInput.get("value");
                    //System.out.println("Inputvalue : " + inputValue);
                    long returnToAddress = 0;
                    if (!currentTxid.equals(prevTxid)) {
                        if (i > 0) {
                            //System.out.println("sendValueTransaction -> " + sendValueTransaction);
                            arrayListSending.add(sendValueTransaction);
                            totalSum += sendValueTransaction;
                        }
                        sendValueTransaction = 0;
                        for (Document dc : vout) {
                            ArrayList<String> docAddresses = (ArrayList<String>) dc.get("addresses");

                            //System.out.println(docAddresses);
                            String localAddress = "";
                            try {
                                localAddress = docAddresses.get(0);
                            } catch (IndexOutOfBoundsException e) {
                            }
                            //System.out.println(localAddress);

                            if (localAddress.equals(address)) {
                                returnToAddress += (long) dc.get("value");
                                //System.out.println("Stesso indirizzo :" + returnToAddress);
                            }
                        }
                    }
                    currentValue = (inputValue - returnToAddress);
                    //double rate = rates.getRates(currentTxid);
                    double rate = (double)transaction.get("rate");

                    totalSendUSD += (rate * ((float) currentValue / 100000000));
                    sendValueTransaction += currentValue;

                    sendValue += currentValue;

                    //System.out.println("SendValue : " + sendValue);
                    prevTxid = currentTxid;

                    i++;

                    if(currentTime<minTime)minTime=currentTime;
                    if(currentTime>maxTime)maxTime=currentTime;

                    ArrayList<String> addressInTransaction = new ArrayList<>();
                    for (Document dc : vout) {
                        for(String addr:(ArrayList<String>) dc.get("addresses")){
                            if (!addr.equals(address) && !addresses.contains(addr)) {
                                addresses.add(addr);
                                differentAddress++;
                            }
                        }
                    }
                } catch (NoSuchElementException e) {
                    //System.out.println("Problema ->" + e);
                } catch (NullPointerException e) {
                    //System.out.println("Problema ->" + e);
                } catch (IndexOutOfBoundsException e) {
                    //System.out.println("Problema ->" + e);
                }
            }
        }
        totalSum += sendValueTransaction;
        arrayListSending.add(sendValueTransaction);

        System.out.println("     NUmberInput: " + arrayListSending.size());
        System.out.println("     InviatoUSD : " + totalSendUSD);
        System.out.println("     InviatoBTC : " + totalSum);
        System.out.println("     Paid  : " + differentAddress);
        ArrayList<Long> time = new ArrayList<>();
        time.add(minTime);
        time.add(maxTime);
        if(totalSum==0)return new Quintuple<>(0, 0l, 0d,0,time);
        return new Quintuple<>(arrayListSending.size(), totalSum, totalSendUSD,differentAddress,time);
    }

}