SELECT COUNT(*)
FROM (
    SELECT DISTINCT MeasurementInfo._createdBy AS [user],
           n.ClaimValue AS user_name,
           MeasurementInfo.ObjectId As measurement,
           MeasurementInfo.ObjectName AS measurement_label,
           TypeInfo.TypeId,
           TypeInfo.TypeName
    FROM objectLinkObject
    JOIN ObjectInfo MeasurementInfo ON MeasurementInfo.ObjectId = objectLinkObject.LinkedObjectId
    JOIN AspNetUserClaims n
        ON MeasurementInfo._createdBy = n.UserId
        AND n.ClaimType = 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name'
    JOIN TypeInfo ON MeasurementInfo.TypeId = TypeInfo.TypeId
    WHERE MeasurementInfo.TypeId NOT IN (
        5, /* Substrate */
        6, /* Sample (pieces) */
        83, /* Request for synthesis */
        89, /* Ideas or experiment plans */
        8 /* Composition */
    )
) AS t