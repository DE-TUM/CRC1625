SELECT
vro.vroObjectLinkObject.ObjectId AS MLId,
compositionInfo.ObjectId AS CompositionId,
compositionLocation.Value AS MeasurementArea
FROM vro.vroObjectLinkObject
/* Composition information */
JOIN vro.vroObjectInfo compositionInfo ON compositionInfo.ObjectId = vro.vroObjectLinkObject.LinkedObjectId
/* ML information */
JOIN vro.vroObjectInfo MLInfo ON MLInfo.ObjectId = vro.vroObjectLinkObject.ObjectId
/* The original measurement the composition has been parsed from points to the composition's object */
JOIN vro.vroObjectLinkObject originalMeasurement ON originalMeasurement.LinkedObjectId = vro.vroObjectLinkObject.LinkedObjectId
/* Original measurement's information */
JOIN vro.vroObjectInfo originalMeasurementInfo ON originalMeasurement.ObjectId = originalMeasurementInfo.ObjectId
/* Composition's location */
JOIN vro.vroPropertyInt compositionLocation ON vro.vroObjectLinkObject.LinkedObjectId = compositionLocation.ObjectId
/* First Handover related to a ML, including also MLs that have *no* handovers */
LEFT JOIN (
    SELECT H.HandoverId, O._created, H.SampleObjectId
    FROM vro.vroHandover H
    /* ObjectInfo of the handover */
    JOIN vro.vroObjectInfo O ON H.HandoverId = O.ObjectId
    /* Handover with the earliest creation date for each ML */
    JOIN (
        SELECT SampleObjectId, MIN(_created) AS min_created
        FROM vro.vroHandover
        JOIN vro.vroObjectInfo ON vro.vroHandover.HandoverId = vro.vroObjectInfo.ObjectId
        GROUP BY SampleObjectId
    ) MinH ON H.SampleObjectId = MinH.SampleObjectId AND O._created = MinH.min_created
) InitialHandover ON vro.vroObjectLinkObject.ObjectId = InitialHandover.SampleObjectId
WHERE MLInfo.TypeId = 6 /* Sample */
AND compositionInfo.TypeId = 8 /* Composition */
AND originalMeasurementInfo.TypeId IN (13, 15, 19, 53, 78, 79) /* EDX */
AND InitialHandover.HandoverId IS NULL
AND compositionLocation.propertyName IN ('Measurement Area', 'MeasurementArea')