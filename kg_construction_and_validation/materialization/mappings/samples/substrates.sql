SELECT ObjectId AS SubstrateObjectId,
       _createdBy AS createdBy,
       FORMAT(_created, 'yyyy-MM-ddTHH:mm:ss.fff') AS created,
       ObjectName,
       ObjectDescription
FROM vro.vroObjectInfo
WHERE TypeId = 5