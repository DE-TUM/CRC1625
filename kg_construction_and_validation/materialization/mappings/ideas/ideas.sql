SELECT ObjectId,
       _createdBy AS createdBy,
       FORMAT(_created, 'yyyy-MM-ddTHH:mm:ss.fff') AS created,
       ObjectName, ObjectDescription FROM vro.vroObjectInfo
WHERE typeId = 89