import java.util.*;
import java.util.stream.Collectors;


public class Main {
    private static MongoConnector mongo;

    public static void main(String[] args) {
        mongo = new MongoConnector();
        mongo.connect();

        long startTime       = System.currentTimeMillis();
        CSVFile csv          = new CSVFile();
        CalculateFeatures cf = new CalculateFeatures();

        ArrayList<Integer> inTx       = new ArrayList<>();
        ArrayList<Integer> outTx      = new ArrayList<>();
        ArrayList<Long> inBTC         = new ArrayList<>();
        ArrayList<Long> outBTC        = new ArrayList<>();
        ArrayList<Double> inUSD       = new ArrayList<>();
        ArrayList<Double> outUSD      = new ArrayList<>();
        ArrayList<Integer> paying     = new ArrayList<>();
        ArrayList<Integer> paid       = new ArrayList<>();
        ArrayList<String> dateFirstTx = new ArrayList<>();
        ArrayList<String> dateLastTx  = new ArrayList<>();
        ArrayList<Integer> lifetime   = new ArrayList<>();

        int iteration = 0;
        ArrayList<String> addresses = csv.getAddresses();
        for (String add : addresses) {
            System.out.println("Address : " + add + "       i:" + iteration);
            try {
                // inTx, inBTC, inUSD, Paying, [FirstDate, LastDate]
                Quintuple<Integer,Long,Double,Integer,ArrayList<Long>> income = cf.getIncome(mongo,add);
                Quintuple<Integer,Long,Double,Integer,ArrayList<Long>> spend  = cf.getSpending(mongo,add);


                inTx.add(income.getFirst());
                outTx.add(spend.getFirst());
                inBTC.add(income.getSecond());
                outBTC.add(spend.getSecond());
                inUSD.add(income.getThird());
                outUSD.add(spend.getThird());
                paying.add(income.getFourth());
                paid.add(spend.getFourth());

                Date firstDate;
                long firstDateLong;
                long lastDateLong;

                if(income.getFifth().get(0)<=spend.getFifth().get(0)){//
                    firstDateLong = income.getFifth().get(0);
                }else{
                    firstDateLong = spend.getFifth().get(0);
                }
                firstDate = new Date(firstDateLong*1000);//from seconds to milliseconds

                Date lastDate;
                if(income.getFifth().get(1)<=spend.getFifth().get(1)){
                    lastDateLong = income.getFifth().get(1);
                }else{
                    lastDateLong = spend.getFifth().get(1);
                }
                lastDate = new Date(lastDateLong*1000);//from seconds to milliseconds

                System.out.println("    Init =" + firstDate.toString());
                int life = (int)(lastDateLong-firstDateLong)/Settings.SECONDS_IN_A_DAY;

                dateFirstTx.add(firstDate.toString());
                dateLastTx.add(lastDate.toString());
                lifetime.add(life);

            } catch (NullPointerException e) {
                System.out.println("PRoblema E: "+e);
            }
            iteration++;

        }
        csv.writeStatistics("Statistics",addresses,inTx,outTx,inBTC,outBTC,inUSD,outUSD,paying,paid,dateFirstTx,dateLastTx,lifetime);

        mongo.disconnect();
        System.out.println("Elapsed time: " + (System.currentTimeMillis() - startTime) / 1000 + " secondi");
        System.out.println("Elapsed time: " + (float) (System.currentTimeMillis() - startTime) / 1000 / 60 + " minuti");
        System.out.println("Elapsed time: " + (float) (System.currentTimeMillis() - startTime) / 1000 / 60 / 60 + " ore");

    }


    /*
    * Method used to order a Map<K, V>
    */
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
}
