SELECT
linkingMLs.ObjectId AS MLId,
measurementData.ObjectId AS MeasurementId
FROM objectLinkObject linkingMLs
JOIN ObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
JOIN ObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
LEFT JOIN (
    SELECT H.HandoverId, H.SampleObjectId
    FROM Handover H
    JOIN ObjectInfo O ON H.HandoverId = O.ObjectId
) InitialHandover ON linkingMLs.ObjectId = InitialHandover.SampleObjectId
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
AND InitialHandover.HandoverId IS NULL