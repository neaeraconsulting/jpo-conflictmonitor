db.ProcessedSpat.updateMany(
    { utcTimeStampTS: { $exists: false }},
    [{ $set: { utcTimeStampTS: { $dateFromString: { dateString: "$utcTimeStamp", onError: null, onNull: null }}}}]
);

db.ProcessedSpat_MV.createIndex({ intersectionID: 1 });
db.ProcessedSpat_MV.createIndex({ utcTimeStampTS: 1 });