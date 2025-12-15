SELECT
('https://crc1625.mdi.ruhr-uni-bochum.de/' +
        CASE
            WHEN source.TypeId = 3 THEN 'literature_reference/'
            WHEN source.TypeId = 4 THEN 'publication/'
            WHEN source.TypeId = 5 THEN 'substrate/'
            WHEN source.TypeId = 6 THEN 'object/'
            WHEN source.TypeId = 83 THEN 'request_for_synthesis/'
            WHEN source.TypeId = 89 THEN 'idea_or_experiment_plan/'
            WHEN source.TypeId = -1 THEN 'handover/'
            ELSE 'measurement/'
        END
    + CONVERT(VARCHAR, source.ObjectId)) AS sourceIRI,

CASE
    WHEN source.TypeId IN (-1, 89) THEN 'https://w3id.org/pmd/co/output'
    WHEN (source.TypeId IN (6, 99) AND target.TypeId IN (6, 99)) THEN 'https://w3id.org/pmd/co/composedOf'
    WHEN (source.TypeId IN (6, 99) AND target.TypeId NOT IN (6, 99)) THEN 'https://w3id.org/pmd/co/characteristic'
    WHEN (source.TypeId NOT IN (6, 99) AND target.TypeId IN (6, 99)) THEN 'https://w3id.org/pmd/co/resource'
    WHEN (source.TypeId NOT IN (6, 99) AND target.TypeId NOT IN (6, 99)) THEN 'https://w3id.org/pmd/co/characteristic'
END AS relation,

('https://crc1625.mdi.ruhr-uni-bochum.de/' +
        CASE
            WHEN target.TypeId = 3 THEN 'literature_reference/'
            WHEN target.TypeId = 4 THEN 'publication/'
            WHEN target.TypeId = 5 THEN 'substrate/'
            WHEN target.TypeId = 6 THEN 'object/'
            WHEN target.TypeId = 83 THEN 'request_for_synthesis/'
            WHEN target.TypeId = 89 THEN 'idea_or_experiment_plan/'
            WHEN target.TypeId = -1 THEN 'handover/'
            ELSE 'measurement/'
        END
    + CONVERT(VARCHAR, target.ObjectId)) AS targetIRI

FROM vro.vroObjectLinkObject
JOIN vro.vroObjectInfo source on vro.vroObjectLinkObject.ObjectId = source.ObjectId
JOIN vro.vroObjectInfo target on vro.vroObjectLinkObject.LinkedObjectId = target.ObjectId
WHERE source.TypeId NOT IN (
    -1, 8, 125 /* Handover, volume or surface composition */
)
AND target.TypeId NOT IN (
    -1, 8, 125 /* Handover, volume or surface composition */
)