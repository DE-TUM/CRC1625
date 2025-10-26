SELECT hnd.HandoverId, hnd.SampleObjectId AS MLId
FROM Handover hnd
JOIN ObjectInfo hndInfo ON hndInfo.ObjectId = hnd.HandoverId
JOIN (
    SELECT SampleObjectId, MIN(_created) AS earliest_date
    FROM Handover
    JOIN ObjectInfo earliestInfo ON earliestInfo.ObjectId = HandoverId
    GROUP BY SampleObjectId
) earliestHandovers
ON hnd.SampleObjectId = earliestHandovers.SampleObjectId
AND hndInfo._created = earliestHandovers.earliest_date