SELECT source.objectId as sourceObjectID, target.objectId AS targetObjectID
FROM ObjectLinkObject
JOIN ObjectInfo source on ObjectLinkObject.ObjectId = source.ObjectId
JOIN ObjectInfo target on ObjectLinkObject.LinkedObjectId = target.ObjectId
WHERE source.TypeId IN (6, 99) AND target.TypeId IN (6, 99) /* Physical samples or computational samples */