SELECT
/* ML, its measurement alongside its creation, and the handover
 it **may** belong to  based on their creation dates */
linkingMLs.ObjectId AS MLId,
measurementData.ObjectId AS MeasurementId,
FORMAT(measurementData._created, 'yyyy-MM-ddTHH:mm:ss.fff') AS MeasurementDate,
handoverData.handoverId AS HandoverId
/* MLs linking to measurements */
FROM objectLinkObject linkingMLs
/* ObjectInfo of the measurements */
JOIN ObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
JOIN ObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
JOIN (
    /* Handovers alongside their creation date and the ML they refer to */
    SELECT ObjectInfo.objectId AS handoverId,
    ObjectInfo._created AS handoverDate,
    Handover.sampleObjectId AS MLId
    FROM ObjectInfo
    JOIN Handover ON ObjectInfo.objectId = Handover.handoverid
    WHERE ObjectInfo.typeId = -1
) handoverData ON linkingMLs.ObjectId = handoverData.MLId
WHERE   measurementData.TypeId IN ({measurement_ids})
AND sampleData.TypeId = 6
/* Get only the handover that has the maximum date among those
   that have a creation date earlier than the measurement's creation date */
AND handoverData.handoverDate = (
    SELECT MAX(hSub._created)
    FROM ObjectInfo hSub
    JOIN Handover hSubData ON hSub.objectId = hSubData.handoverid
    WHERE hSubData.sampleObjectId = linkingMLs.ObjectId
    AND hSub._created < measurementData._created
    AND hSub.typeId = -1
)