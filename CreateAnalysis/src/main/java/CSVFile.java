import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Sergio Serusi on 20/03/2017.
 */
public class CSVFile {
    //CSV
    private static final String CSVBasePath = "CSV//";
    private static final String CSVAddressesPath = "CSV//PonziAddresses.csv";
    private String COMMA_DEL;
    private String NEW_LINE;
    private String FILE_HEAD;
    private String SPACE;
    private FileWriter fileWriter;
    private BufferedReader fileReader;


    CSVFile() {
        fileWriter = null;
        fileReader = null;
        COMMA_DEL = ",";
        SPACE = " ";
        NEW_LINE = "\n";
        FILE_HEAD = "day,value";
    }


    /*
    * The method getAddresses read the addresses from a file and get an ArrayList containing that addresses
    * @return ArrayList<String> of addresses
     */
    ArrayList<String> getAddresses() {
        ArrayList<String> addresses = new ArrayList<>();
        try {
            String line;
            fileReader = new BufferedReader(new FileReader(CSVAddressesPath));
            fileReader.readLine();
            while ((line = fileReader.readLine()) != null) {
                // use comma as separator
                String[] cols = line.split(COMMA_DEL);
                addresses.add(cols[1]);
            }
        } catch (Exception e) {
            System.out.println("Error in CsvFileReader!");
            e.printStackTrace();
        } finally {
            try {
                fileReader.close();
            } catch (IOException e) {
                System.out.println("Error while flushing/closing fileReader!");
                e.printStackTrace();
            }
        }
        return addresses;

    }

    /*
    * The method getDailySend receive an address and get the an HashMap<Integer, Long> containing the coin send by the address day by day
    * @param String The address
    * @return HashMap<Integer,Long> of the coin send by the address day by day
    */
    HashMap<Integer, Long> getDailySend(String address) {
        HashMap<Integer, Long> dailySend = new HashMap<>();

        try {
            String line;
            fileReader = new BufferedReader(new FileReader(CSVBasePath + "Send\\Money\\" + address + ".csv"));
            fileReader.readLine();
            while ((line = fileReader.readLine()) != null) {
                // use comma as separator
                String[] cols = line.split(COMMA_DEL);
                int day = Integer.parseInt(cols[0]);
                long value = Long.parseLong(cols[1]);
                dailySend.put(day, value);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error in CsvFileReader!");
            //e.printStackTrace();
        } catch (IOException e) {
            //e.printStackTrace();
        } finally {
            try {
                fileReader.close();
            } catch (IOException e) {
                System.out.println("Error while flushing/closing fileReader!");
                e.printStackTrace();
            }
        }

        return dailySend;
    }

    /*
    * The method getDailyBalance Balance an address and get the an HashMap<Integer, Long> containing the coin send by the address day by day
    * @param String The address
    * @return HashMap<Integer,Long> of the coin send by the address day by day
    */
    HashMap<Integer, Long> getDailyBalance(String address) {
        HashMap<Integer, Long> dailySend = new HashMap<>();

        try {
            String line;
            fileReader = new BufferedReader(new FileReader(CSVBasePath + "Balance\\Money\\" + address + ".csv"));
            fileReader.readLine();
            while ((line = fileReader.readLine()) != null) {
                // use comma as separator
                String[] cols = line.split(COMMA_DEL);
                int day = Integer.parseInt(cols[0]);
                long value = Long.parseLong(cols[1]);
                dailySend.put(day, value);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error in CsvFileReader!");
            //e.printStackTrace();
        } catch (IOException e) {
            //e.printStackTrace();
        } finally {
            try {
                fileReader.close();
            } catch (IOException e) {
                System.out.println("Error while flushing/closing fileReader!");
                e.printStackTrace();
            }
        }
        return dailySend;
    }

    /*
    * The method getDailyBalance Balance an address and get the an HashMap<Integer, Long> containing the coin send by the address day by day
    * @param String The address
    * @return HashMap<Integer,Long> of the coin send by the address day by day
    */
    HashMap<Integer, Long> getDailyBalance(String address, String path) {
        HashMap<Integer, Long> dailySend = new HashMap<>();

        try {
            String line;
            fileReader = new BufferedReader(new FileReader(CSVBasePath + path + address + ".csv"));
            fileReader.readLine();
            while ((line = fileReader.readLine()) != null) {
                // use comma as separator
                String[] cols = line.split(COMMA_DEL);
                int day = Integer.parseInt(cols[0]);
                long value = Long.parseLong(cols[1]);
                dailySend.put(day, value);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error in CsvFileReader!");
            //e.printStackTrace();
        } catch (IOException e) {
            //e.printStackTrace();
        } finally {
            try {
                fileReader.close();
            } catch (IOException e) {
                System.out.println("Error while flushing/closing fileReader!");
                e.printStackTrace();
            }
        }
        return dailySend;
    }

    void writeStatistics(String nameFile, ArrayList<String> address,
                         ArrayList<Integer> inTx,
                         ArrayList<Integer> outTx,
                         ArrayList<Long> inBTC,
                         ArrayList<Long> outBTC,
                         ArrayList<Double> inUSD,
                         ArrayList<Double> outUSD,
                         ArrayList<Integer> paying,
                         ArrayList<Integer> paid,
                         ArrayList<String> dateFirstTx,
                         ArrayList<String> dateLastTx,
                         ArrayList<Integer> lifetime
                         ) {
        try {
            fileWriter = new FileWriter(CSVBasePath  + nameFile + ".csv");

            //Write the CSV file header
            fileWriter.append("address,inTx,outTx,inBTC,outBTC,inUSD,outUSD,paying,paid,dateFirstTx,dateLastTx,lifetime");

            //Add a new line separator after the header
            fileWriter.append(NEW_LINE);
            for (int i = 0; i < address.size(); i++) {
                fileWriter.append(address.get(i));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(Integer.toString(inTx.get(i)));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(Integer.toString(outTx.get(i)));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(Long.toString(inBTC.get(i)));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(Long.toString(outBTC.get(i)));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(Double.toString(inUSD.get(i)));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(Double.toString(outUSD.get(i)));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(Integer.toString(paying.get(i)));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(Integer.toString(paid.get(i)));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(dateFirstTx.get(i));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(dateLastTx.get(i));
                fileWriter.append(COMMA_DEL);
                fileWriter.append(Integer.toString(lifetime.get(i)));
                fileWriter.append(NEW_LINE);
            }
        } catch (Exception e) {
            System.out.println("Error in CsvFileWriter !!!");
            e.printStackTrace();
        } finally {
            try {
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                System.out.println("Error while flushing/closing fileWriter !!!");
                e.printStackTrace();
            }
        }
    }


}
