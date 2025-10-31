SELECT ObjectInfo.ObjectId,
    CASE
        /* https://crc1625.mdi.ruhr-uni-bochum.de/rubric/inf_templates_sample_type */
        WHEN propertyInt.Value = 0 THEN 'https://w3id.org/pmd/co/EngineeredMaterial' /* 'Unknown' */
        WHEN propertyInt.Value = 1 THEN 'https://crc1625.mdi.ruhr-uni-bochum.de/MaterialsLibrary'
        WHEN propertyInt.Value = 2 THEN 'https://crc1625.mdi.ruhr-uni-bochum.de/StripeSample'
        WHEN propertyInt.Value = 3 THEN 'https://crc1625.mdi.ruhr-uni-bochum.de/NoGradientSample'
        WHEN propertyInt.Value = 4 THEN 'https://crc1625.mdi.ruhr-uni-bochum.de/StressChip'
        WHEN propertyInt.Value = 5 THEN 'https://crc1625.mdi.ruhr-uni-bochum.de/Sample'
        /* Not included: 6 - https://crc1625.mdi.ruhr-uni-bochum.de/object/nano-particles-24916 */
        ELSE 'https://w3id.org/pmd/co/EngineeredMaterial' /* Objects with no property also fall here, although there are none (for now) */
    END AS SampleType
FROM ObjectInfo
LEFT JOIN PropertyInt ON ObjectInfo.ObjectId = PropertyInt.ObjectId
WHERE ObjectInfo.TypeId = 6 /* Sample */
AND propertyInt.Value IN (1, 2, 3, 4 , 5) /* The rest will be pmdco:EngineeredMaterial, via samples_workflow_instances_and_initial_work_templated.yml */
AND PropertyInt.PropertyName = 'Type'