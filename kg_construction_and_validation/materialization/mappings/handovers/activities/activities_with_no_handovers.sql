SELECT
linkingMLs.ObjectId AS MLId,
measurementData.ObjectId AS MeasurementId
FROM vro.vroObjectLinkObject linkingMLs
JOIN vro.vroObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
JOIN vro.vroObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
LEFT JOIN (
    SELECT H.HandoverId, H.SampleObjectId
    FROM vro.vroHandover H
    JOIN vro.vroObjectInfo O ON H.HandoverId = O.ObjectId
) InitialHandover ON linkingMLs.ObjectId = InitialHandover.SampleObjectId
WHERE measurementData.TypeId IN ({measurement_ids})
AND sampleData.TypeId = 6
AND InitialHandover.HandoverId IS NULL