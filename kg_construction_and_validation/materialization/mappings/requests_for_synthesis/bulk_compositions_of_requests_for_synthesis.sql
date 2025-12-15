SELECT SampleId AS ObjectId, ElementName, ValuePercent FROM vro.vroComposition
JOIN vro.vroObjectInfo ON vro.vroObjectInfo.ObjectId = SampleId
WHERE vro.vroObjectInfo.TypeId = 83