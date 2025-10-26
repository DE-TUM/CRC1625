SELECT objectLinkObject.ObjectId AS MLId,
       REPLACE(objectInfo.objectName, '%', '_') AS SubstrateName,
       objectInfo.objectId AS SubstrateObjectId,
       ObjectInfo._createdBy AS createdBy,
       FORMAT(ObjectInfo._created, 'yyyy-MM-ddTHH:mm:ss.fff') AS created,
       ObjectInfo.ObjectName,
       ObjectInfo.ObjectDescription FROM objectLinkObject
JOIN ObjectInfo ON ObjectInfo.ObjectId = objectLinkObject.LinkedObjectId
WHERE ObjectInfo.TypeId = 5