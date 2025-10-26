SELECT COUNT(*)
FROM (
    /* equivalent to activities.sql */
    SELECT
    /* ML, its measurement alongside its creation, and the handover
     it **may** belong to  based on their creation dates */
    linkingMLs.ObjectId AS MLId,
    handoverData.handoverId AS HandoverId
    /* MLs linking to measurements */
    FROM objectLinkObject linkingMLs
    /* ObjectInfo of the measurements */
    JOIN ObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
    JOIN ObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
    JOIN (
        /* Handovers alongside their creation date and the ML they refer to */
        SELECT ObjectInfo.objectId AS handoverId,
        ObjectInfo._created AS handoverDate,
        Handover.sampleObjectId AS MLId
        FROM ObjectInfo
        JOIN Handover ON ObjectInfo.objectId = Handover.handoverid
        WHERE ObjectInfo.typeId = -1
    ) handoverData ON linkingMLs.ObjectId = handoverData.MLId
    WHERE measurementData.TypeId IN (17, 31, 44, 55, 56, 97)
    AND sampleData.TypeId = 6
    /* Get only the handover that has the maximum date among those
       that have a creation date earlier than the measurement's creation date */
    AND handoverData.handoverDate = (
        SELECT MAX(hSub._created)
        FROM ObjectInfo hSub
        JOIN Handover hSubData ON hSub.objectId = hSubData.handoverid
        WHERE hSubData.sampleObjectId = linkingMLs.ObjectId
        AND hSub._created < measurementData._created
        AND hSub.typeId = -1
    )

    UNION

    /* equivalent to activities_prior_to_first_handover.sql */
    SELECT
    linkingMLs.ObjectId AS MLId,
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
    WHERE measurementData.TypeId IN (17, 31, 44, 55, 56, 97)
    AND sampleData.TypeId = 6
    AND measurementData._created < InitialHandover._created

    UNION

    /* equivalent to activities_with_no_handovers.sql */
    SELECT
    linkingMLs.ObjectId AS MLId,
    1 AS HandoverId
    FROM objectLinkObject linkingMLs
    JOIN ObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
    JOIN ObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
    LEFT JOIN (
        SELECT H.HandoverId, H.SampleObjectId
        FROM Handover H
        JOIN ObjectInfo O ON H.HandoverId = O.ObjectId
    ) InitialHandover ON linkingMLs.ObjectId = InitialHandover.SampleObjectId
    WHERE measurementData.TypeId IN (17, 31, 44, 55, 56, 97)
    AND sampleData.TypeId = 6
    AND InitialHandover.HandoverId IS NULL
) AS t