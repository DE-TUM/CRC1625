SELECT
FORMAT(compositionInfo._created, 'yyyyMMddHHmmssfff') AS CompositionEpoch,
handoverData.HandoverId,
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
JOIN PropertyInt compositionLocation ON compositionLocation.ObjectId = ObjectLinkObject.LinkedObjectId
JOIN (
    /* Handovers alongside their creation date and the ML they refer to */
    SELECT ObjectInfo.objectId AS handoverId,
    ObjectInfo._created AS handoverDate,
    Handover.sampleObjectId AS MLId
    FROM ObjectInfo
    JOIN Handover ON ObjectInfo.objectId = Handover.handoverid
    WHERE ObjectInfo.typeId = -1
    ) handoverData ON ObjectLinkObject.ObjectId = handoverData.MLId
WHERE MLInfo.TypeId = 6 /* Sample */
AND compositionInfo.TypeId = 8 /* Composition */
AND originalMeasurementInfo.TypeId IN (13, 15, 19, 53, 78, 79) /* EDX */
/* Get only the handover that has the maximum date among those
   that have a creation date earlier than the measurement's creation date */
AND handoverData.handoverDate = (
    SELECT MAX(hSub._created)
    FROM ObjectInfo hSub
    JOIN Handover hSubData ON hSub.objectId = hSubData.handoverid
    WHERE hSubData.sampleObjectId = ObjectLinkObject.ObjectId
    AND hSub._created < compositionInfo._created
    AND hSub.typeId = -1
)
AND compositionLocation.propertyName IN ('Measurement Area', 'MeasurementArea')