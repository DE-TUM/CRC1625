SELECT SampleId,
       value AS Element
FROM vro.vroSample
JOIN vro.vroObjectInfo ON vro.vroObjectInfo.ObjectId = vro.vroSample.SampleId
CROSS APPLY STRING_SPLIT(SUBSTRING(Elements, 2, LEN(Elements) - 2), '-')
WHERE vro.vroObjectInfo.TypeId IN (6, 99) /* Only samples, exclude the compositions */