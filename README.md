# BitcoinPonzi

We have transformed the raw Bitcoin blockchain into a MongoDB database containing the main information used for our analysis.
The structure or information used are shown below in the section "Schema". 
Given the large size of the database, we recommend create indexes to reduce calculation time. We have been using the indexes in the section below ("Indexes").

## Instructions
### Indexes
After the execution on of the program, to achieve a reasonable speed you have to create some indexes.
Run the queries listed below (time ~45' each).

* db.transaction.createIndex({"txid" : 1})
* db.transaction.createIndex({"vin.address" : 1})
* db.transaction.createIndex({"vout.addresses" : 1})
* db.transaction.createIndex({"time" : 1})
* db.transaction.createIndex({"blockHash" : 1})

### Schema
Transaction collection:
* txid (string): transaction id
* locktime (integer)
* vin (array of documents)
  - sequence (integer): transaction sequence number
  - txid (string): id of the redeemed transaction
  - vout (integer): index of the redeemed output
  - address (string): address of the redeemer (optional, works only for pay-to-publickey-hash
* vout (array of documents)
  - value (integer): output value in satoshi
  - n (integer): output index
  - addresses (array of string): addresses that can redeem the output (array to handle multisig scripts)
* blockhash (string): id of the containing block 
* blockheight (integer)
* time (long)
* rate (long): BTC = rate USD


These (information in the transaction) are the minimal information you need.
