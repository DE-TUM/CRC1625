SELECT UserId,
       CASE
           /* If a user belongs to multiple groups, then we concatenate them, e.g.: A01_B01 */
           WHEN COUNT(ClaimValue) = 1 THEN MAX(ClaimValue)
           ELSE STRING_AGG(ClaimValue, '_') WITHIN GROUP (ORDER BY ClaimValue)
       END AS ClaimValue
FROM AspNetUserClaims
WHERE ClaimType = 'Project'
GROUP BY UserId;