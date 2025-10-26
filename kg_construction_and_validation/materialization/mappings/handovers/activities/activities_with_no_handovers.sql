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
WHERE measurementData.TypeId IN ({measurement_ids})
AND sampleData.TypeId = 6
AND InitialHandover.HandoverId IS NULL