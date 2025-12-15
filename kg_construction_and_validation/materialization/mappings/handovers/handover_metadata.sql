SELECT
HandoverId,
_createdBy AS createdBy,
DestinationUserId,
FORMAT(_created, 'yyyy-MM-ddTHH:mm:ss.fff') AS created,
FORMAT(DestinationConfirmed, 'yyyy-MM-ddTHH:mm:ss.fff') AS DestinationConfirmed,
ObjectName,
ObjectDescription
FROM vro.vroHandover
JOIN vro.vroObjectInfo ON HandoverId = ObjectId