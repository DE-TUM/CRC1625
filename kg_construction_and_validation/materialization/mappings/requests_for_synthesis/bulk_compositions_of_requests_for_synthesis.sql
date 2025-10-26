SELECT SampleId AS ObjectId, ElementName, ValuePercent FROM composition
JOIN ObjectInfo ON ObjectInfo.ObjectId = SampleId
WHERE ObjectInfo.TypeId = 83