SELECT ObjectLinkObject.ObjectId as MLId,
       MeasurementInfo.ObjectId As MeasurementId,
       MeasurementInfo._createdBy AS createdBy,
       FORMAT(MeasurementInfo._created, 'yyyy-MM-ddTHH:mm:ss.fff') AS created,
       MeasurementInfo.ObjectFilePath,
       MeasurementInfo.ObjectName,
       MeasurementInfo.ObjectDescription,
       TypeInfo.TypeId,
       TypeInfo.TypeName
FROM ObjectLinkObject
JOIN ObjectInfo SampleInfo ON SampleInfo.ObjectId = ObjectLinkObject.ObjectId
JOIN ObjectInfo MeasurementInfo ON MeasurementInfo.ObjectId = ObjectLinkObject.LinkedObjectId
JOIN TypeInfo ON MeasurementInfo.TypeId = TypeInfo.TypeId
WHERE MeasurementInfo.TypeId NOT IN (
    5, /* Substrate */
    6, /* Sample (pieces) */
    83, /* Request for synthesis */
    89, /* Ideas or experiment plans */
    8 /* Composition */
)
AND SampleInfo.TypeId = 6