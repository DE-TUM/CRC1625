SELECT COUNT(*)
FROM (
    SELECT
        u.Id AS [user],
        n.ClaimValue AS name,
        ObjectInfo.ObjectId AS ML,
        ObjectInfo.ObjectName AS ML_label,
        ObjectInfo.ObjectDescription AS ML_comment
    FROM AspNetUsers u
    JOIN AspNetUserClaims n
        ON u.Id = n.UserId
        AND n.ClaimType = 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name'
    JOIN ObjectInfo ON ObjectInfo._createdBy = u.Id
    WHERE ObjectInfo.TypeId = 6 /* Materials Library */
) AS t