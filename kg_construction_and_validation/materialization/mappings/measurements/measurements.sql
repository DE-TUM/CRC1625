SELECT vro.vroObjectInfo.ObjectId As MeasurementId,
       vro.vroObjectInfo._createdBy AS createdBy,
       FORMAT(vro.vroObjectInfo._created, 'yyyy-MM-ddTHH:mm:ss.fff') AS created,
       vro.vroObjectInfo.ObjectFilePath,
       vro.vroObjectInfo.ObjectName,
       vro.vroObjectInfo.ObjectDescription,
       vro.vroTypeInfo.TypeId,
       vro.vroTypeInfo.TypeName
FROM vro.vroObjectInfo
JOIN vro.vroTypeInfo ON vro.vroObjectInfo.TypeId = vro.vroTypeInfo.TypeId
WHERE vro.vroObjectInfo.TypeId NOT IN (
    -1, /* Handover */
    3, 4, /* Literature reference or publication */
    5, /* Substrate */
    6, 99, /* Object (MLs, samples, computational samples) */
    83, /* Request for synthesis */
    89, /* Ideas or experiment plans */
    8, 125 /* Volume or surface composition */
)