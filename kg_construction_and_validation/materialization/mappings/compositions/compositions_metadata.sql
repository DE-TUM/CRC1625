SELECT
vro.vroObjectLinkObject.ObjectId AS MLId,
vro.vroObjectLinkObject.LinkedObjectId AS CompositionId,
compositionInfo.ObjectId AS CompositionId,
originalMeasurement.ObjectId AS OriginalMeasurementId,
compositionValues.ElementName,
FORMAT(compositionValues.ValuePercent, '0.0000') AS ValuePercent, /* Fixed to a string with 4 decimal places, to avoid weird formatting errors */
compositionLocation.Value AS MeasurementArea
FROM vro.vroObjectLinkObject
/* The original measurement the composition has been parsed from points to the composition's object */
JOIN vro.vroObjectLinkObject originalMeasurement ON originalMeasurement.LinkedObjectId = vro.vroObjectLinkObject.LinkedObjectId
/* Original measurement's information */
JOIN vro.vroObjectInfo originalMeasurementInfo ON originalMeasurement.ObjectId = originalMeasurementInfo.ObjectId
/* Composition information */
JOIN vro.vroObjectInfo compositionInfo ON vro.vroObjectLinkObject.LinkedObjectId = compositionInfo.ObjectId
/* ML information */
JOIN vro.vroObjectInfo MLInfo ON vro.vroObjectLinkObject.ObjectId = MLInfo.ObjectId
/* Composition elements and percentages */
JOIN vro.vroComposition compositionValues ON vro.vroObjectLinkObject.LinkedObjectId = compositionValues.SampleId
/* Composition's location */
JOIN vro.vroPropertyInt compositionLocation ON vro.vroObjectLinkObject.LinkedObjectId = compositionLocation.ObjectId
WHERE MLInfo.TypeId = 6 /* Sample */
AND compositionInfo.TypeId = 8 /* Composition */
AND originalMeasurementInfo.TypeId IN (13, 15, 19, 53, 78, 79) /* EDX */
AND compositionLocation.propertyName IN ('Measurement Area', 'MeasurementArea')