

const config = {
	// cluster information
	clusterName: "demo.aamtz.mongodb.net",

	// source collection information
	dbName: "sample_weatherdata",
	sourceColName: "data",
	partitionFieldName: "ts",	// what about partitioning on multiple fields?
	partitionFieldType: "date",		// can have the following values: "number", "date", "objectid"

	// the list of indexes to be built at start. Useful if you repeated drop a collection during testing
	enforceIndexes: [							
		{
			colName: "data",
			indexDef: {ts : 1},
			indexOptions: {name: "ts_1"}
		},
/*		
		{
			colName: "flatData",
			indexDef: {ts: 1},
			indexOptions: {name: "ts_1", unique: true}
			}
			*/
	],

	// parallelization information
	totalNumPartitions: 1000,
	numThreads: 10,
	
	// other configuration fields as necessary. These are typically used to parameterize agg query in transPipeline variable

	destColName: "flatData", 			// collection results are being written to (if appropriate)

}

const transPipeline = [
	//should include all aggregation stages except for initial $match stage
  {
    $project: {
      _id: 0,
      ts: 1,
      measurements: [
        {
          k: "airTemperature",
          v: "$airTemperature",
        },
        {
          k: "dewPoint",
          v: "$dewPoint",
        },
        {
          k: "pressure",
          v: "$pressure",
        },
        {
          k: "wind",
          v: "$wind",
        },
        {
          k: "visibility",
          v: "$visibility",
        },
        {
          k: "skyCondition",
          v: "$skyCondition",
        },
      ],
      header: {
        st: "$st",
        position: "$position",
        elevation: "$elevation",
        callLetters: "$callLetters",
        dataSource: "$dataSource",
        type: "$type",
        sections: "$sections",
      },
    },
  },
  {
    $unwind: {
      path: "$measurements",
    },
  },
  {
    /**
     * replacementDocument: A document or string.
     */
    $replaceRoot: {
      newRoot: {
        $mergeObjects: [
          {
            ts: "$ts",
            header: "$header",
          },
          {
            $arrayToObject: [["$measurements"]],
          },
        ],
      },
    },
  },
	];


module.exports = { config, transPipeline};
