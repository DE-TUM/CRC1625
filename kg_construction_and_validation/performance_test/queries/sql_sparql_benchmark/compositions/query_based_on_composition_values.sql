SELECT COUNT(*)
FROM (
    SELECT
    ObjectLinkObject.ObjectId AS MLId,
    originalMeasurement.ObjectId AS OriginalMeasurementId,
    compositionLocation.Value AS MeasurementArea,
    compositionValues.ValuePercent
    FROM ObjectLinkObject
    /* The original measurement the composition has been parsed from points to the composition's object */
    JOIN ObjectLinkObject originalMeasurement ON originalMeasurement.LinkedObjectId = ObjectLinkObject.LinkedObjectId
    /* Original measurement's information */
    JOIN ObjectInfo originalMeasurementInfo ON originalMeasurement.ObjectId = originalMeasurementInfo.ObjectId
    /* Composition information */
    JOIN ObjectInfo compositionInfo ON ObjectLinkObject.LinkedObjectId = compositionInfo.ObjectId
    /* ML information */
    JOIN ObjectInfo MLInfo ON ObjectLinkObject.ObjectId = MLInfo.ObjectId
    /* Composition elements and percentages */
    JOIN Composition compositionValues ON ObjectLinkObject.LinkedObjectId = compositionValues.SampleId
    /* Composition's location */
    JOIN PropertyInt compositionLocation ON ObjectLinkObject.LinkedObjectId = compositionLocation.ObjectId
    WHERE MLInfo.TypeId = 6 /* Sample */
    AND compositionInfo.TypeId = 8 /* Composition */
    AND originalMeasurementInfo.TypeId IN (13, 15, 19, 53, 78, 79) /* EDX */
    AND compositionLocation.propertyName IN ('Measurement Area', 'MeasurementArea')
    AND compositionValues.ValuePercent > 50.0
    AND compositionValues.ElementName = 'Au'
) AS t