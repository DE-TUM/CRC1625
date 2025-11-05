SELECT ObjectInfo.ObjectId As MeasurementId,
       ObjectInfo._createdBy AS createdBy,
       FORMAT(ObjectInfo._created, 'yyyy-MM-ddTHH:mm:ss.fff') AS created,
       ObjectInfo.ObjectFilePath,
       ObjectInfo.ObjectName,
       ObjectInfo.ObjectDescription,
       TypeInfo.TypeId,
       TypeInfo.TypeName
FROM ObjectInfo
JOIN TypeInfo ON ObjectInfo.TypeId = TypeInfo.TypeId
WHERE ObjectInfo.TypeId NOT IN (
    -1, /* Handover */
    3, 4, /* Literature reference or publication */
    5, /* Substrate */
    6, 99, /* Object (MLs, samples, computational samples) */
    83, /* Request for synthesis */
    89, /* Ideas or experiment plans */
    8, 125 /* Volume or surface composition */
)