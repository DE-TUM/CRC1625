SELECT COUNT(*)
FROM (
    SELECT
        u.Id AS UserId,
        n.ClaimValue AS Name,
        COUNT(ObjectInfo.ObjectId) AS itemCount
    FROM AspNetUsers u
    JOIN AspNetUserClaims n
        ON u.Id = n.UserId
        AND n.ClaimType = 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name'
    JOIN ObjectInfo ON ObjectInfo._createdBy = u.Id
    WHERE ObjectInfo.TypeId = 6 /* Materials Library */
    GROUP BY u.Id, n.ClaimValue
    /*ORDER BY itemCount DESC*/
) AS t