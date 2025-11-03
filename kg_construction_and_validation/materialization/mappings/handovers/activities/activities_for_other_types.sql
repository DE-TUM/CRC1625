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
WHERE
measurementData.TypeId NOT IN (12) /* Photo */
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
    5, /* Substrate */
    6, /* Sample (pieces) */
    83, /* Request for synthesis */
    89, /* Ideas or experiment plans */
    8 /* Composition */
)
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