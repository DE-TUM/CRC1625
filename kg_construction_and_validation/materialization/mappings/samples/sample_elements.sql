SELECT SampleId,
       value AS Element
FROM Sample
JOIN ObjectInfo ON ObjectInfo.ObjectId = Sample.SampleId
CROSS APPLY STRING_SPLIT(SUBSTRING(Elements, 2, LEN(Elements) - 2), '-')
WHERE ObjectInfo.TypeId = 6 /* Only samples, exclude the compositions */