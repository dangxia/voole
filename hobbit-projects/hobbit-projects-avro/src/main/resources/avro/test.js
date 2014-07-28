var k = {
	"type" : "record",
	"name" : "OrderPlayAliveReqV2",
	"namespace" : "com.voole.hobbit.avro.termial",
	"fields" : [ {
		"name" : "sessID",
		"type" : [ "string", "null" ]
	}, {
		"name" : "adjPlayTime",
		"type" : [ "long", "null" ]
	}, {
		"name" : "accID",
		"type" : [ "long", "null" ]
	}, {
		"name" : "aliveTick",
		"type" : [ "long", "null" ]
	}, {
		"name" : "seekNum",
		"type" : [ "long", "null" ]
	}, {
		"name" : "readNum",
		"type" : [ "long", "null" ]
	}, {
		"name" : "unsuccRead",
		"type" : [ "long", "null" ]
	}, {
		"name" : "readPos",
		"type" : [ "long", "null" ]
	}, {
		"name" : "sessAvgSpeed",
		"type" : [ "long", "null" ]
	}, {
		"name" : "linkNum",
		"type" : [ "int", "null" ]
	}, {
		"name" : "_srvs",
		"type" : [ "null", {
			"type" : "array",
			"items" : {
				"type" : "record",
				"name" : "OrderPlayAliveReqSrvV2",
				"fields" : [ {
					"name" : "srvIP",
					"type" : [ "long", "null" ]
				}, {
					"name" : "connTimes",
					"type" : [ "long", "null" ]
				}, {
					"name" : "transNum",
					"type" : [ "long", "null" ]
				}, {
					"name" : "avgRTT",
					"type" : [ "long", "null" ]
				}, {
					"name" : "accBytes",
					"type" : [ "long", "null" ]
				}, {
					"name" : "accTime",
					"type" : [ "long", "null" ]
				}, {
					"name" : "avgSpeed",
					"type" : [ "long", "null" ]
				} ]
			}
		} ]
	} ]
}