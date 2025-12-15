SELECT
/* ML, its measurement alongside its creation, and the handover
 it **may** belong to  based on their creation dates */
linkingMLs.ObjectId AS MLId,
measurementData.ObjectId AS MeasurementId,
FORMAT(measurementData._created, 'yyyy-MM-ddTHH:mm:ss.fff') AS MeasurementDate,
handoverData.handoverId AS HandoverId
/* MLs linking to measurements */
FROM vro.vroObjectLinkObject linkingMLs
/* ObjectInfo of the measurements */
JOIN vro.vroObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
JOIN vro.vroObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
JOIN (
    /* Handovers alongside their creation date and the ML they refer to */
    SELECT vro.vroObjectInfo.objectId AS handoverId,
    vro.vroObjectInfo._created AS handoverDate,
    vro.vroHandover.sampleObjectId AS MLId
    FROM vro.vroObjectInfo
    JOIN vro.vroHandover ON vro.vroObjectInfo.objectId = vro.vroHandover.handoverid
    WHERE vro.vroObjectInfo.typeId = -1
) handoverData ON linkingMLs.ObjectId = handoverData.MLId
WHERE NOT EXISTS ( /* Exclude measurements that are already linked to a handover */
    SELECT 1
    FROM vro.vroObjectLinkObject
    JOIN vro.vroObjectInfo s on vro.vroObjectLinkObject.ObjectId = s.ObjectId
    WHERE s.TypeId = -1 AND vro.vroObjectLinkObject.LinkedObjectId = measurementData.ObjectId
)
AND measurementData.TypeId IN ({measurement_ids})
AND sampleData.TypeId = 6
/* Get only the handover that has the maximum date among those
   that have a creation date earlier than the measurement's creation date */
AND handoverData.handoverDate = (
    SELECT MAX(hSub._created)
    FROM vro.vroObjectInfo hSub
    JOIN vro.vroHandover hSubData ON hSub.objectId = hSubData.handoverid
    WHERE hSubData.sampleObjectId = linkingMLs.ObjectId
    AND hSub._created < measurementData._created
    AND hSub.typeId = -1
)