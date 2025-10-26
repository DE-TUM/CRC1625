SELECT
/* ML + its measurement alongside its creation that was performed
   strictly before the first handover creation, and the first handover's ID */
linkingMLs.ObjectId AS MLId,
measurementData.ObjectId AS MeasurementId,
FORMAT(measurementData._created, 'yyyy-MM-ddTHH:mm:ss.fff') AS MeasurementDate,
InitialHandover.HandoverId AS HandoverId
/* MLs linking to measurements */
FROM objectLinkObject linkingMLs
/* ObjectInfo of the measurements */
JOIN ObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
JOIN ObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
/* First Handover related to a ML, including also MLs that have *no* handovers */
JOIN (
    SELECT H.HandoverId, O._created, H.SampleObjectId
    FROM Handover H
    /* ObjectInfo of the handover */
    JOIN ObjectInfo O ON H.HandoverId = O.ObjectId
    /* Handover with the earliest creation date for each ML */
    JOIN (
        SELECT SampleObjectId, MIN(_created) AS min_created
        FROM Handover
        JOIN ObjectInfo ON Handover.HandoverId = ObjectInfo.ObjectId
        GROUP BY SampleObjectId
    ) MinH ON H.SampleObjectId = MinH.SampleObjectId AND O._created = MinH.min_created
) InitialHandover ON linkingMLs.ObjectId = InitialHandover.SampleObjectId
WHERE   measurementData.TypeId NOT IN (13, 15, 19, 53, 78, 79) /* EDX */
AND measurementData.TypeId NOT IN (17, 31, 44, 55, 56, 97) /* XRD */
AND measurementData.TypeId NOT IN (30) /* XPS */
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
AND measurementData.TypeId NOT IN (
    5, /* Substrate */
    6, /* Sample (pieces) */
    83, /* Request for synthesis */
    89, /* Ideas or experiment plans */
    8 /* Composition */
)
AND sampleData.TypeId = 6
/* Either the measurement has an earlier creation date than the first handover,
   or there are no handovers at all */
AND measurementData._created < InitialHandover._created