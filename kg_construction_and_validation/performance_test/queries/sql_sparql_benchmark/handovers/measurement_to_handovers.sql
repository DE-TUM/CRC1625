SELECT COUNT(*)
FROM (
    /* activities.sql */
    SELECT
    /* ML, its measurement alongside its creation, and the handover
     it **may** belong to  based on their creation dates */
    linkingMLs.ObjectId AS MLId,
    measurementData.ObjectId AS MeasurementId,
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
    WHERE   measurementData.TypeId IN (13, 15, 19, 53, 78, 79,
    17, 31, 44, 55, 56, 97,
    30,
    48,
    27, 38, 39, 40,
    24,
    14, 16, 33,
    41, 80, 81, 82,
    20,
    26,
    57, 58, 85,
    50, 59, 60, 86, 87,
    47)
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

    /* activities_for_other_types.sql */
    SELECT
    /* ML, its measurement alongside its creation, and the handover
     it **may** belong to  based on their creation dates */
    linkingMLs.ObjectId AS MLId,
    measurementData.ObjectId AS MeasurementId,
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
    WHERE   measurementData.TypeId NOT IN (13, 15, 19, 53, 78, 79) /* EDX */
    AND measurementData.TypeId NOT IN (17, 31, 44, 55, 56, 97) /* XRD */
    AND measurementData.TypeId NOT IN (30) /* XPS */
    AND measurementData.TypeId NOT IN (48) /* LEIS */
    AND measurementData.TypeId NOT IN (27, 38, 39, 40) /* Thickness */
    AND measurementData.TypeId NOT IN (24) /* SEM */
    AND measurementData.TypeId NOT IN (14, 16, 33) /* Resistance */
    AND measurementData.TypeId NOT IN (41, 80, 81, 82) /* Bandgap */
    AND measurementData.TypeId NOT IN (20) /* APT */
    AND measurementData.TypeId NOT IN (26) /* TEM */
    AND measurementData.TypeId NOT IN (57, 58, 85) /* SDC */
    AND measurementData.TypeId NOT IN (50, 59, 60, 86, 87) /* SECCM */
    AND measurementData.TypeId NOT IN (47) /* FIM */
    AND measurementData.TypeId NOT IN (
        5, /* Substrate */
        6, /* Sample (pieces) */
        83, /* Request for synthesis */
        89, /* Ideas or experiment plans */
        8 /* Composition */
    )
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

    /* activities_for_other_types_prior_to_first_handover.sq l*/
    SELECT
    /* ML + its measurement alongside its creation that was performed
       strictly before the first handover creation, and the first handover's ID */
    linkingMLs.ObjectId AS MLId,
    measurementData.ObjectId AS MeasurementId,
    InitialHandover.HandoverId AS HandoverId
    /* MLs linking to measurements */
    FROM objectLinkObject linkingMLs
    /* ObjectInfo of the measurements */
    JOIN ObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
    JOIN ObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
    /* First Handover related to a ML, including also MLs that have *no* handovers */
    JOIN (
        SELECT H.HandoverId, O._created, H.SampleObjectId
        FROM Handover H
        /* ObjectInfo of the handover */
        JOIN ObjectInfo O ON H.HandoverId = O.ObjectId
        /* Handover with the earliest creation date for each ML */
        JOIN (
            SELECT SampleObjectId, MIN(_created) AS min_created
            FROM Handover
            JOIN ObjectInfo ON Handover.HandoverId = ObjectInfo.ObjectId
            GROUP BY SampleObjectId
        ) MinH ON H.SampleObjectId = MinH.SampleObjectId AND O._created = MinH.min_created
    ) InitialHandover ON linkingMLs.ObjectId = InitialHandover.SampleObjectId
    WHERE   measurementData.TypeId NOT IN (13, 15, 19, 53, 78, 79) /* EDX */
    AND measurementData.TypeId NOT IN (17, 31, 44, 55, 56, 97) /* XRD */
    AND measurementData.TypeId NOT IN (30) /* XPS */
    AND measurementData.TypeId NOT IN (48) /* LEIS */
    AND measurementData.TypeId NOT IN (27, 38, 39, 40) /* Thickness */
    AND measurementData.TypeId NOT IN (24) /* SEM */
    AND measurementData.TypeId NOT IN (14, 16, 33) /* Resistance */
    AND measurementData.TypeId NOT IN (41, 80, 81, 82) /* Bandgap */
    AND measurementData.TypeId NOT IN (20) /* APT */
    AND measurementData.TypeId NOT IN (26) /* TEM */
    AND measurementData.TypeId NOT IN (57, 58, 85) /* SDC */
    AND measurementData.TypeId NOT IN (50, 59, 60, 86, 87) /* SECCM */
    AND measurementData.TypeId NOT IN (47) /* FIM */
    AND measurementData.TypeId NOT IN (
        5, /* Substrate */
        6, /* Sample (pieces) */
        83, /* Request for synthesis */
        89, /* Ideas or experiment plans */
        8 /* Composition */
    )
    AND sampleData.TypeId = 6
    /* Either the measurement has an earlier creation date than the first handover,
       or there are no handovers at all */
    AND measurementData._created < InitialHandover._created


    UNION

    /* activities_for_other_types_with_no_handovers.sql */
    SELECT
    linkingMLs.ObjectId AS MLId,
    measurementData.ObjectId AS MeasurementId,
    1 AS HandoverId
    FROM objectLinkObject linkingMLs
    JOIN ObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
    JOIN ObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
    LEFT JOIN (
        SELECT H.HandoverId, H.SampleObjectId
        FROM Handover H
        JOIN ObjectInfo O ON H.HandoverId = O.ObjectId
    ) InitialHandover ON linkingMLs.ObjectId = InitialHandover.SampleObjectId
    WHERE   measurementData.TypeId NOT IN (13, 15, 19, 53, 78, 79) /* EDX */
    AND measurementData.TypeId NOT IN (17, 31, 44, 55, 56, 97) /* XRD */
    AND measurementData.TypeId NOT IN (30) /* XPS */
    AND measurementData.TypeId NOT IN (48) /* LEIS */
    AND measurementData.TypeId NOT IN (27, 38, 39, 40) /* Thickness */
    AND measurementData.TypeId NOT IN (24) /* SEM */
    AND measurementData.TypeId NOT IN (14, 16, 33) /* Resistance */
    AND measurementData.TypeId NOT IN (41, 80, 81, 82) /* Bandgap */
    AND measurementData.TypeId NOT IN (20) /* APT */
    AND measurementData.TypeId NOT IN (26) /* TEM */
    AND measurementData.TypeId NOT IN (57, 58, 85) /* SDC */
    AND measurementData.TypeId NOT IN (50, 59, 60, 86, 87) /* SECCM */
    AND measurementData.TypeId NOT IN (47) /* FIM */
    AND measurementData.TypeId NOT IN (
        5, /* Substrate */
        6, /* Sample (pieces) */
        83, /* Request for synthesis */
        89, /* Ideas or experiment plans */
        8 /* Composition */
    )
    AND sampleData.TypeId = 6
    AND InitialHandover.HandoverId IS NULL


    UNION

    /* activities_prior_to_first_handover.sql */
    SELECT
    linkingMLs.ObjectId AS MLId,
    measurementData.ObjectId AS MeasurementId,
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
    WHERE measurementData.TypeId IN (13, 15, 19, 53, 78, 79,
    17, 31, 44, 55, 56, 97,
    30,
    48,
    27, 38, 39, 40,
    24,
    14, 16, 33,
    41, 80, 81, 82,
    20,
    26,
    57, 58, 85,
    50, 59, 60, 86, 87,
    47)
    AND sampleData.TypeId = 6
    AND measurementData._created < InitialHandover._created

    UNION

    /* activities_with_no_handovers.sql */
    SELECT
    linkingMLs.ObjectId AS MLId,
    measurementData.ObjectId AS MeasurementId,
    1 AS HandoverId
    FROM objectLinkObject linkingMLs
    JOIN ObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
    JOIN ObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId
    LEFT JOIN (
        SELECT H.HandoverId, H.SampleObjectId
        FROM Handover H
        JOIN ObjectInfo O ON H.HandoverId = O.ObjectId
    ) InitialHandover ON linkingMLs.ObjectId = InitialHandover.SampleObjectId
    WHERE measurementData.TypeId IN (13, 15, 19, 53, 78, 79,
    17, 31, 44, 55, 56, 97,
    30,
    48,
    27, 38, 39, 40,
    24,
    14, 16, 33,
    41, 80, 81, 82,
    20,
    26,
    57, 58, 85,
    50, 59, 60, 86, 87,
    47)
    AND sampleData.TypeId = 6
    AND InitialHandover.HandoverId IS NULL
) AS t