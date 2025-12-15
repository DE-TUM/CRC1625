SELECT
compositionInfo.ObjectId AS CompositionId,
handoverData.HandoverId,
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
JOIN vro.vroPropertyInt compositionLocation ON compositionLocation.ObjectId = vro.vroObjectLinkObject.LinkedObjectId
JOIN (
    /* Handovers alongside their creation date and the ML they refer to */
    SELECT vro.vroObjectInfo.objectId AS handoverId,
    vro.vroObjectInfo._created AS handoverDate,
    vro.vroHandover.sampleObjectId AS MLId
    FROM vro.vroObjectInfo
    JOIN vro.vroHandover ON vro.vroObjectInfo.objectId = vro.vroHandover.handoverid
    WHERE vro.vroObjectInfo.typeId = -1
    ) handoverData ON vro.vroObjectLinkObject.ObjectId = handoverData.MLId
WHERE MLInfo.TypeId = 6 /* Sample */
AND compositionInfo.TypeId = 8 /* Composition */
AND originalMeasurementInfo.TypeId IN (13, 15, 19, 53, 78, 79) /* EDX */
/* Get only the handover that has the maximum date among those
   that have a creation date earlier than the measurement's creation date */
AND handoverData.handoverDate = (
    SELECT MAX(hSub._created)
    FROM vro.vroObjectInfo hSub
    JOIN vro.vroHandover hSubData ON hSub.objectId = hSubData.handoverid
    WHERE hSubData.sampleObjectId = vro.vroObjectLinkObject.ObjectId
    AND hSub._created < compositionInfo._created
    AND hSub.typeId = -1
)
AND compositionLocation.propertyName IN ('Measurement Area', 'MeasurementArea')