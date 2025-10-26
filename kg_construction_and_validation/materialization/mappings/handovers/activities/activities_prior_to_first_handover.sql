SELECT
linkingMLs.ObjectId AS MLId,
measurementData.ObjectId AS MeasurementId,
FORMAT(measurementData._created, 'yyyy-MM-ddTHH:mm:ss.fff') AS MeasurementDate,
InitialHandover.HandoverId AS HandoverId
FROM objectLinkObject linkingMLs
JOIN ObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
JOIN ObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
JOIN (
    SELECT H.HandoverId, O._created, H.SampleObjectId
    FROM Handover H
    JOIN ObjectInfo O ON H.HandoverId = O.ObjectId
    JOIN (
        SELECT SampleObjectId, MIN(_created) AS min_created
        FROM Handover
        JOIN ObjectInfo ON Handover.HandoverId = ObjectInfo.ObjectId
        GROUP BY SampleObjectId
    ) MinH ON H.SampleObjectId = MinH.SampleObjectId AND O._created = MinH.min_created
) InitialHandover ON linkingMLs.ObjectId = InitialHandover.SampleObjectId
WHERE measurementData.TypeId IN ({measurement_ids})
AND sampleData.TypeId = 6
AND measurementData._created < InitialHandover._created