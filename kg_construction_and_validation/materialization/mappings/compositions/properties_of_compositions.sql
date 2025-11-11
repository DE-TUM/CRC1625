SELECT compositionInfo.ObjectId AS CompositionId,
    CASE
        WHEN compositionMetadata.propertyname = 'x' THEN 'x_position'
        WHEN compositionMetadata.propertyname = 'y' THEN 'y_position'
        WHEN compositionMetadata.propertyname = 'R' THEN 'resistance'
        WHEN compositionMetadata.propertyname = 'Tolerance' THEN 'tolerance'
        ELSE compositionMetadata.propertyname
    END AS propertyname,
FORMAT(compositionMetadata.value, '0.0000') AS value, /* Fixed to a string with 4 decimal places, to avoid weird formatting errors */
compositionLocation.value  AS MeasurementArea
FROM ObjectLinkObject
/* The original measurement the composition has been parsed from points to the composition's object */
JOIN ObjectLinkObject originalMeasurement ON originalMeasurement.linkedobjectid = ObjectLinkObject.LinkedObjectId
/* Original measurement's information */
JOIN objectinfo originalMeasurementInfo ON originalMeasurement.ObjectId = originalMeasurementInfo.ObjectId
/* Composition information */
JOIN objectinfo compositionInfo ON compositionInfo.ObjectId = ObjectLinkObject.LinkedObjectId
/* ML information */
JOIN objectinfo MLInfo ON MLInfo.ObjectId = ObjectLinkObject.ObjectId
/* Composition's extra metadata (R, tolerance, tool's x,y positions...) */
JOIN PropertyFloat compositionMetadata ON compositionMetadata.ObjectId = ObjectLinkObject.LinkedObjectId
/* Composition's location */
JOIN PropertyInt compositionLocation ON compositionLocation.ObjectId = objectlinkobject.LinkedObjectId
WHERE  MLInfo.TypeId = 6 /* Sample  */
AND compositionInfo.TypeId = 8 /* Composition */
AND originalMeasurementInfo.TypeId IN ( 13, 15, 19, 53, 78, 79 ) /* EDX */
AND compositionLocation.propertyName IN ('Measurement Area', 'MeasurementArea')
AND compositionMetadata.propertyName IN ('x', 'y', 'R', 'Tolerance')