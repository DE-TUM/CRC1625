SELECT
h1.objectid AS HandoverId,
h2.objectid AS NextHandoverId
FROM
objectinfo h1
JOIN objectinfo h2 ON h1._created < h2._created
JOIN handover hnd1 ON h1.objectid = hnd1.handoverid
JOIN handover hnd2 ON h2.objectid = hnd2.handoverid
WHERE
    hnd1.sampleobjectid = hnd2.sampleobjectid
    /* The date must be the earliest one among the following handovers after h1 */
    AND h2._created = (
        SELECT MIN(h2b._created)
        FROM objectinfo h2b
        JOIN handover hnd2b ON h2b.objectid = hnd2b.handoverid
        WHERE
        h2b._created > h1._created
        AND h2b.typeid = -1
        AND hnd2b.sampleobjectid = hnd1.sampleobjectid
    )