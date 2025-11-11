SELECT
compositionInfo.ObjectId AS CompositionId,
InitialHandover.HandoverId,
compositionLocation.Value AS MeasurementArea
FROM ObjectLinkObject
/* Composition information */
JOIN ObjectInfo compositionInfo ON compositionInfo.ObjectId = ObjectLinkObject.LinkedObjectId
/* ML information */
JOIN ObjectInfo MLInfo ON MLInfo.ObjectId = ObjectLinkObject.ObjectId
/* The original measurement the composition has been parsed from points to the composition's object */
JOIN ObjectLinkObject originalMeasurement ON originalMeasurement.LinkedObjectId = ObjectLinkObject.LinkedObjectId
/* Original measurement's information */
JOIN ObjectInfo originalMeasurementInfo ON originalMeasurement.ObjectId = originalMeasurementInfo.ObjectId
/* Composition's location */
JOIN PropertyInt compositionLocation ON ObjectLinkObject.LinkedObjectId = compositionLocation.ObjectId
/* First Handover related to a ML */
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
) InitialHandover ON ObjectLinkObject.ObjectId = InitialHandover.SampleObjectId
WHERE MLInfo.TypeId = 6 /* Sample */
AND compositionInfo.TypeId = 8 /* Composition */
AND originalMeasurementInfo.TypeId IN (13, 15, 19, 53, 78, 79) /* EDX */
/* Get only the handover that has the maximum date among those
   that have a creation date earlier than the measurement's creation date */
AND compositionInfo._created < InitialHandover._created
AND compositionLocation.propertyName IN ('Measurement Area', 'MeasurementArea')
