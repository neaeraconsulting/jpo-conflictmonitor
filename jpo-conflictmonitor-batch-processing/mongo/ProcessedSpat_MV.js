// Create a materialized view with utcTimestamp and odeReceivedAt as timestamps with indexes
// Run in mongosh
db.ProcessedSpat.aggregate([
    { $set: { utcTimeStamp: { $dateFromString: { dateString: "$utcTimeStamp", onError: null, onNull: null }}}},
    { $set: { odeReceivedAt: { $dateFromString: { dateString: "$odeReceivedAt", onError: null, onNull: null }}}},
    { $merge: { into: "ProcessedSpat_MV", whenMatched: "replace", whenNotMatched: "insert"}}
]);

db.ProcessedSpat_MV.createIndex({ intersectionID: 1 });
db.ProcessedSpat_MV.createIndex({ utcTimeStamp: 1 });
db.ProcessedSpat_MV.createIndex({ odeReceivedAt: 1 });
db.ProcessedSpat_MV.createIndex({ recordGeneratedAt: 1 });