SELECT COUNT(*)
FROM (
    SELECT
        u.Id AS [user],
        n.ClaimValue AS user_name,
        ObjectInfo.ObjectId AS measurement,
        ObjectInfo.ObjectName AS measurement_label
    FROM AspNetUsers u
    JOIN AspNetUserClaims n
        ON u.Id = n.UserId
        AND n.ClaimType = 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name'
    JOIN ObjectInfo ON ObjectInfo._createdBy = u.Id
    WHERE ObjectInfo.TypeId = 13 /* Materials Library */
) AS t