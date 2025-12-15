SELECT
measurementData.ObjectId AS MeasurementId,
handoverData.ObjectId AS HandoverId

FROM vro.vroObjectLinkObject linkingMLs

/* MLs or samples linking to measurements */
JOIN vro.vroObjectInfo measurementData ON measurementData.ObjectId = linkingMLs.LinkedObjectId
JOIN vro.vroObjectInfo sampleData ON sampleData.ObjectId = linkingMLs.ObjectId

/* Where there is an explicit handover -> measurement link */
JOIN vro.vroObjectLinkObject linkingHandovers ON linkingHandovers.LinkedObjectId = measurementData.ObjectId
JOIN vro.vroObjectInfo handoverData ON handoverData.ObjectId = linkingHandovers.ObjectId

WHERE handoverData.TypeId = -1
AND measurementData.TypeId IN ({measurement_ids})
AND sampleData.TypeId = 6
