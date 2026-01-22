SELECT vro.vroObjectInfo.ObjectId,
    CASE
        /* https://crc1625.mdi.ruhr-uni-bochum.de/rubric/inf_templates_sample_type */
        WHEN vro.vroPropertyInt.Value = 0 THEN 'https://w3id.org/pmd/co/EngineeredMaterial' /* 'Unknown' */
        WHEN vro.vroPropertyInt.Value = 1 THEN 'https://crc1625.mdi.ruhr-uni-bochum.de/MaterialsLibrary'
        WHEN vro.vroPropertyInt.Value = 2 THEN 'https://crc1625.mdi.ruhr-uni-bochum.de/StripeSample'
        WHEN vro.vroPropertyInt.Value = 3 THEN 'https://crc1625.mdi.ruhr-uni-bochum.de/NoGradientSample'
        WHEN vro.vroPropertyInt.Value = 4 THEN 'https://crc1625.mdi.ruhr-uni-bochum.de/StressChip'
        WHEN vro.vroPropertyInt.Value = 5 THEN 'https://crc1625.mdi.ruhr-uni-bochum.de/Sample'
        /* Not included: 6 - https://crc1625.mdi.ruhr-uni-bochum.de/object/nano-particles-24916 */
        ELSE 'https://w3id.org/pmd/co/EngineeredMaterial' /* Objects with no property also fall here, although there are none (for now) */
    END AS SampleType
FROM vro.vroObjectInfo
LEFT JOIN vro.vroPropertyInt ON vro.vroObjectInfo.ObjectId = vro.vroPropertyInt.ObjectId
WHERE vro.vroObjectInfo.TypeId = 6 /* Sample */
AND vro.vroPropertyInt.Value IN (1, 2, 3, 4 , 5) /* The rest will be crc:EngineeredMaterial, via samples_workflow_instances_and_initial_work_templated.yml */
AND vro.vroPropertyInt.PropertyName = 'Type'