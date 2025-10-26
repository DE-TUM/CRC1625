SELECT SampleId, externalID,
       FORMAT(_created, 'yyyy-MM-ddTHH:mm:ss.fff') AS created,
       _createdBy AS createdBy,
       ObjectName,
       ObjectDescription
FROM objectInfo
JOIN Sample ON ObjectId = SampleId
WHERE TypeId = 6