SELECT SampleId,
       externalID,
       FORMAT(_created, 'yyyy-MM-ddTHH:mm:ss.fff') AS created,
       _createdBy AS createdBy,
       ObjectName,
       ObjectDescription,
       TypeId
FROM vro.vroObjectInfo
JOIN vro.vroSample ON ObjectId = SampleId
WHERE TypeId IN (6, 99) /* Physical samples or computational samples */