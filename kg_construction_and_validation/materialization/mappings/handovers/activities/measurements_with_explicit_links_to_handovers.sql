SELECT
measurementData.ObjectId AS MeasurementId,
handoverData.ObjectId AS HandoverId

FROM ObjectLinkObject linkingMLs

/* MLs or samples linking to measurements */
JOIN ObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
JOIN ObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId

/* Where there is an explicit handover -> measurement link */
JOIN ObjectLinkObject linkingHandovers ON linkingHandovers.LinkedObjectId = measurementData.ObjectId
JOIN ObjectInfo handoverData ON handoverData.ObjectId = linkingHandovers.ObjectId

WHERE handoverData.TypeId = -1
AND measurementData.TypeId IN ({measurement_ids})
AND sampleData.TypeId = 6
