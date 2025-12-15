SELECT hnd.HandoverId, hnd.SampleObjectId AS MLId
FROM vro.vroHandover hnd
JOIN vro.vroObjectInfo hndInfo ON hndInfo.ObjectId = hnd.HandoverId
JOIN (
    SELECT SampleObjectId, MIN(_created) AS earliest_date
    FROM vro.vroHandover
    JOIN vro.vroObjectInfo earliestInfo ON earliestInfo.ObjectId = HandoverId
    GROUP BY SampleObjectId
) earliestHandovers
ON hnd.SampleObjectId = earliestHandovers.SampleObjectId
AND hndInfo._created = earliestHandovers.earliest_date