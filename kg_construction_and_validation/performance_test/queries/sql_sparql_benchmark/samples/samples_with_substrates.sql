SELECT COUNT(*)
FROM (
    SELECT
        objectLinkObject.ObjectId AS ML, /* It's the same as ?internalID, as we don't have URIs here */
        SampleInfo.ObjectId AS internalID,
        SampleInfo._created AS creationDate,
        SampleInfo._createdBy AS [user],
        SubstrateInfo.ObjectName AS substrate_label
    FROM ObjectLinkObject
    JOIN ObjectInfo SampleInfo ON SampleInfo.ObjectId = ObjectLinkObject.ObjectId
    JOIN ObjectInfo SubstrateInfo ON SubstrateInfo.ObjectId = ObjectLinkObject.LinkedObjectId
    WHERE SampleInfo.TypeId = 6 /* Substrate */
    AND SubstrateInfo.TypeId = 5 /* Materials Library */
) AS t