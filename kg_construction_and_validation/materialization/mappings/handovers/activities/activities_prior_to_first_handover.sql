SELECT
linkingMLs.ObjectId AS MLId,
measurementData.ObjectId AS MeasurementId,
FORMAT(measurementData._created, 'yyyy-MM-ddTHH:mm:ss.fff') AS MeasurementDate,
InitialHandover.HandoverId AS HandoverId
FROM vro.vroObjectLinkObject linkingMLs
JOIN vro.vroObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
JOIN vro.vroObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
JOIN (
    SELECT H.HandoverId, O._created, H.SampleObjectId
    FROM vro.vroHandover H
    JOIN vro.vroObjectInfo O ON H.HandoverId = O.ObjectId
    JOIN (
        SELECT SampleObjectId, MIN(_created) AS min_created
        FROM vro.vroHandover
        JOIN vro.vroObjectInfo ON vro.vroHandover.HandoverId = vro.vroObjectInfo.ObjectId
        GROUP BY SampleObjectId
    ) MinH ON H.SampleObjectId = MinH.SampleObjectId AND O._created = MinH.min_created
) InitialHandover ON linkingMLs.ObjectId = InitialHandover.SampleObjectId
WHERE NOT EXISTS ( /* Exclude measurements that are already linked to a handover */
    SELECT 1
    FROM vro.vroObjectLinkObject
    JOIN vro.vroObjectInfo s on vro.vroObjectLinkObject.ObjectId = s.ObjectId
    WHERE s.TypeId = -1 AND vro.vroObjectLinkObject.LinkedObjectId = measurementData.ObjectId
)
AND measurementData.TypeId IN ({measurement_ids})
AND sampleData.TypeId = 6
AND measurementData._created < InitialHandover._created