SELECT COUNT(*)
FROM (
    SELECT
        u.Id AS UserId,
        n.ClaimValue AS Name,
        g.ClaimValue AS GivenName,
        s.ClaimValue AS Surname,
        p.ClaimValue AS Project
    FROM AspNetUsers u
    LEFT JOIN AspNetUserClaims n
        ON u.Id = n.UserId
        AND n.ClaimType = 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name'
    LEFT JOIN AspNetUserClaims g
        ON u.Id = g.UserId
        AND g.ClaimType = 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname'
    LEFT JOIN AspNetUserClaims s
        ON u.Id = s.UserId
        AND s.ClaimType = 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname'
    LEFT JOIN (
        SELECT UserId,
               CASE
                   WHEN COUNT(ClaimValue) = 1 THEN MAX(ClaimValue)
                   ELSE STRING_AGG(ClaimValue, '_') WITHIN GROUP (ORDER BY ClaimValue)
               END AS ClaimValue
        FROM AspNetUserClaims
        WHERE ClaimType = 'Project'
        GROUP BY UserId
    ) p ON u.Id = p.UserId
) AS t