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
AND measurementData.TypeId NOT IN (12) /* Photo */
AND measurementData.TypeId NOT IN (13, 15, 19, 53, 78, 79) /* EDX */
AND measurementData.TypeId NOT IN (17, 31, 44, 55, 56, 97) /* XRD */
AND measurementData.TypeId NOT IN (30) /* XPS */
AND measurementData.TypeId NOT IN (18) /* Annealing */
AND measurementData.TypeId NOT IN (48) /* LEIS */
AND measurementData.TypeId NOT IN (27, 38, 39, 40) /* Thickness */
AND measurementData.TypeId NOT IN (24) /* SEM */
AND measurementData.TypeId NOT IN (14, 16, 33) /* Resistance */
AND measurementData.TypeId NOT IN (41, 80, 81, 82) /* Bandgap */
AND measurementData.TypeId NOT IN (20) /* APT */
AND measurementData.TypeId NOT IN (26) /* TEM */
AND measurementData.TypeId NOT IN (57, 58, 85) /* SDC */
AND measurementData.TypeId NOT IN (50, 59, 60, 86, 87) /* SECCM */
AND measurementData.TypeId NOT IN (47) /* FIM */
AND measurementData.TypeId NOT IN (147) /* PSM */
AND measurementData.TypeId NOT IN (96, 98, 107, 139) /* Report */
AND measurementData.TypeId NOT IN (
    3, 4, /* Literature reference or publication */
    5, /* Substrate */
    6, 99, /* Object (MLs, samples, computational samples) */
    83, /* Request for synthesis */
    89, /* Ideas or experiment plans */
    8, 125 /* Volume or surface composition */
)
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