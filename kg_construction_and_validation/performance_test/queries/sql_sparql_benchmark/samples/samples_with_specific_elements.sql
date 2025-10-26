SELECT COUNT(*)
FROM (
    SELECT SampleId
        FROM Sample
        JOIN ObjectInfo ON ObjectInfo.ObjectId = Sample.SampleId
        CROSS APPLY STRING_SPLIT(SUBSTRING(Sample.Elements, 2, LEN(Sample.Elements) - 2), '-') v2
        WHERE ObjectInfo.TypeId = 6 /* Materials Library */
        GROUP BY Sample.SampleId
        HAVING COUNT(CASE WHEN v2.value = 'Au' THEN 1 END) > 0
           AND COUNT(CASE WHEN v2.value = 'Ag' THEN 1 END) > 0
) AS t