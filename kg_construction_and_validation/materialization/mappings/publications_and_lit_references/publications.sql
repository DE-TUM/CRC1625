SELECT ObjectId,
       _createdBy AS createdBy,
       FORMAT(_created, 'yyyy-MM-ddTHH:mm:ss.fff') AS created,
       ObjectName, ObjectDescription FROM ObjectInfo
WHERE typeId = 4