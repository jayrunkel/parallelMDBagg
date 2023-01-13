// ================================================================
// Installation instructions
// ================================================================
//
// 1. Install nodejs
// 2. Install required node modules
//
//    npm install mongodb
//    npm install async
//    npm install @supercharge/promise-pool
//
// ================================================================
// Execution Instructions
// ================================================================
//
// Run like:
// node parallelTransformBatch.js <dbUser> <dbPassword> <dbName> <clusterURI>
//
// Example:
// node parallelTransformBatch.js user password recommendations cluster0.gptft.mongodb.net



//const { MongoClient, ObjectID, getTimestamp, ISODate} = require("mongodb");
const { MongoClient } = require("mongodb");
//const { async, parallel } = require('async');
const { PromisePool } = require('@supercharge/promise-pool');
const { config, transPipeline} = require('./queryDef.js');


const userName = process.argv[2];
const password = process.argv[3];





const uri = `mongodb+srv://${userName}:${password}@${config.clusterName}?retryWrites=true&w=majority`;

Array.prototype.sortByProp = function(p){
  return this.sort(function(a,b){
    return (a[p] > b[p]) ? 1 : (a[p] < b[p]) ? -1 : 0;
  });
};



const client = new MongoClient(uri);

async function buildIndexes(db, indexList) {
	indexList.forEach(async indexDef => {
		const col = db.collection(indexDef.colName);
		await col.createIndex(indexDef.indexDef, indexDef.indexOptions);
	});
}

async function calcPartitionSize(col, partitionField) {

	const ascendSortDef = {};
	const descendSortDef = {};
	ascendSortDef[partitionField] = 1;
	descendSortDef[partitionField] = -1;
	
	const minDoc = await col.find().sort(ascendSortDef).limit(1).toArray();
	const maxDoc = await col.find().sort(descendSortDef).limit(1).toArray();

	return {
		minDocValue: minDoc[0][partitionField],
		maxDocValue: maxDoc[0][partitionField],
		partitionSize: Math.ceil((maxDocValue - minDocValue)/config.totalNumPartitions)
	};

}

async function processPartition(bId, lowerObjectId, upperObjectId) {
	console.log(`Queing Count[${bId}]: ${lowerObjectId} to ${upperObjectId}`);

	let matchStage = {
		$match : {XTRA_CARD_NBR: {$gte: lowerObjectId, $lte: upperObjectId}}
	};


	const count = await sourceCol.aggregate([matchStage].concat(transPipeline), {allowDiskUse: true}).toArray();

	console.log(`Completed [${bId}]: ${lowerObjectId} to ${upperObjectId} Result: ${count}`);
	return {blockId: bId, lowerBound: lowerObjectId, upperBound: upperObjectId, result: count};
}

function buildPartitions(minDocVal, pSize) {

	let partitionMin;
	let partitionMax;
	const partitionList = [];

	for (let i = 0; i < config.totalNumPartitions; i++) {
			partitionMin = (i === 0) ? minDocVal : partitionMax;
			partitionMax = partitionMin + pSize;
			
			console.log(`Partition ${i} - Min: ${partitionMin}, Max: ${partitionMax}`);
			
			partitionList.push({partitionId: i, lower: partitionMin, upper: partitionMax});
	}

	return partitionList;
}

async function run() {
  try {
		console.log(`Connecting to: ${uri}`);
		const database = client.db(config.dbName);
		
    const sourceCol = database.collection(config.sourceColName);
		try {
			await database.createCollection(config.destColName);
		} catch (err) {
			console.log(`ERROR Creating Collection ${config.destColName}:  err`);
		}

		await buildIndexes(database, config.enforceIndexes);

		const {minDocValue, maxDocValue, partitionSize} = await calcPartitionSize(sourceCol, config.partitionFieldName);

		console.log("================ Partition Calculations ================");
		console.log(`[${config.sourceColName} Collection ${config.partitionFieldName} Range] - Min: ${minDocValue}, Max: ${maxDocValue}`);
		console.log("Partition Size: ", partitionSize);
		
		
		console.log("================ Building Agg Partitions ================");
		const partitionList = buildPartitions(minDocValue, partitionSize);


		console.log("================ Batch Processing Asynchronously ================");
		const {results, errors}  = await PromisePool
					.withConcurrency(config.numThreads)
					.for(partitionList)
					.process(async part => {
						const result = await processPartition(part.partitionId, part.lower, part.upper);
						return result;
					});

		console.log("================ PROCESSING COMPLETE ================");
		console.log("RESULTS: ");
		results.sortByProp('partitionId').map(result => console.log(result));
		console.log("ERRORS: ");
		console.log(errors);
		
  } catch (err) {
		console.log("CAUGHT ERRORS: ");
		console.log(err)
		console.log("CURRENT RESULTS: ");
		results.sortByProp('partitionId').map(result => console.log(result));
	}
	finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
}
run().catch(console.dir);
