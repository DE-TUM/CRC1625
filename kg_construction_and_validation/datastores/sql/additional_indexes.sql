-- Additional indexes that speed up materialization

-- WHERE clauses on ObjectLinkObject
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ObjectLinkObject_ObjectId_LinkedObjectId'
      AND object_id = OBJECT_ID('dbo.ObjectLinkObject')
)
BEGIN
    CREATE INDEX IX_ObjectLinkObject_ObjectId_LinkedObjectId
        ON dbo.ObjectLinkObject (ObjectId, LinkedObjectId);
END;

-- WHERE clauses on TypeId
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ObjectInfo_ObjectId_TypeId'
      AND object_id = OBJECT_ID('dbo.ObjectInfo')
)
BEGIN
    CREATE INDEX IX_ObjectInfo_ObjectId_TypeId
        ON dbo.ObjectInfo (ObjectId, TypeId);
END;

-- WHERE/JOIN clauses on propertyInt (Compositions...)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_PropertyInt_ObjectId_propertyName'
      AND object_id = OBJECT_ID('dbo.PropertyInt')
)
BEGIN
    CREATE INDEX IX_PropertyInt_ObjectId_propertyName
        ON dbo.PropertyInt (ObjectId, propertyName);
END;

-- WHERE/JOIN clauses on propertyFloat (Composition properties...)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_PropertyFloat_ObjectId_propertyName'
      AND object_id = OBJECT_ID('dbo.PropertyFloat')
)
BEGIN
    CREATE INDEX IX_PropertyFloat_ObjectId_propertyName
        ON dbo.PropertyFloat (ObjectId, propertyName);
END;

-- WHERE/JOIN clauses based on ObjectInfo's creation dates (Handovers...)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ObjectInfo_ObjectId_created'
      AND object_id = OBJECT_ID('dbo.ObjectInfo')
)
BEGIN
    CREATE INDEX IX_ObjectInfo_ObjectId_created
        ON dbo.ObjectInfo (ObjectId, _created);
END;
-- WHERE/JOIN clauses based on Sample IDs exclusively
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_Handover_SampleObjectId'
      AND object_id = OBJECT_ID('dbo.Handover')
)
BEGIN
    CREATE INDEX IX_Handover_SampleObjectId
        ON Handover (SampleObjectId);
END;

-- WHERE/JOIN clauses based on Handover IDs and Sample IDs
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_Handover_HandoverId_SampleObjectId'
      AND object_id = OBJECT_ID('dbo.Handover')
)
BEGIN
    CREATE INDEX IX_Handover_HandoverId_SampleObjectId
        ON dbo.Handover (HandoverId, SampleObjectId);
END;