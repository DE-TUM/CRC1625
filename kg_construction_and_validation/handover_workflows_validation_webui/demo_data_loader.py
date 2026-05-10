import os

from datastores.rdf import rdf_datastore_client
from workflows_validation.CRC_1625_workflows_validator.CRC_1625_workflows_validator import CRC1625WorkflowModelStep, set_creator_user_id, crc_prefix, crc_handover_prefix
from workflows_validation.workflow_instance import StepAssignment, WorkflowInstance, store_workflow_instance
from workflows_validation.workflow_model import store_workflow_model, WorkflowModel

module_dir = os.path.dirname(__file__)

prefixes: str = open(os.path.join(module_dir, '../workflows_validation/queries/prefixes.sparql')).read()
are_there_workflow_models_from_demo_user_query = prefixes + open(os.path.join(module_dir, 'queries/are_there_workflow_models_from_demo_user.sparql'), 'r').read()

async def create_demo_workflow_1():
    demo_workflow_model = WorkflowModel()
    demo_workflow_model.create_new_iri()
    demo_workflow_model.name = "Demo workflow 1"
    demo_workflow_model.description = "This is a linear demo workflow"
    demo_workflow_model.workflow_model_steps = {}
    set_creator_user_id(demo_workflow_model, -1)

    step_1 = CRC1625WorkflowModelStep()
    step_1.create_new_iri()
    step_1.name = "Step 1"
    step_1.description = "ML Synthesis and photo"
    step_1.set_allowed_activity_names(["Photo", "Report"])
    step_1.set_allowed_project_names(["A01"])

    step_2 = CRC1625WorkflowModelStep()
    step_2.create_new_iri()
    step_2.name = "Step 2"
    step_2.description = "Initial characterization (EDX and XRD)"
    step_2.set_allowed_activity_names(["EDX", "XRD"])
    step_2.set_allowed_project_names(["S"])

    step_3 = CRC1625WorkflowModelStep()
    step_3.create_new_iri()
    step_3.name = "Step 3"
    step_3.description = "Electrochemical screening"
    step_3.set_allowed_activity_names(["SECCM"])
    step_3.set_allowed_project_names(["C01"])

    step_4 = CRC1625WorkflowModelStep()
    step_4.create_new_iri()
    step_4.name = "Step 4"
    step_4.description = "Special sample synthesis and photo by A02"
    step_4.set_allowed_activity_names(["Photo", "Report"])
    step_4.set_allowed_project_names(["A02"])

    step_5 = CRC1625WorkflowModelStep()
    step_5.create_new_iri()
    step_5.name = "Step 5"
    step_5.description = "Initial characterization (EDX and XRD)"
    step_5.set_allowed_activity_names(["EDX", "XRD"])
    step_5.set_allowed_project_names(["S"])

    step_6 = CRC1625WorkflowModelStep()
    step_6.create_new_iri()
    step_6.name = "Step 6"
    step_6.description = "Electrochemical screening"
    step_6.set_allowed_activity_names(["SECCM"])
    step_6.set_allowed_project_names(["C01"])

    step_1.next_steps = [step_2.iri]
    step_2.next_steps = [step_3.iri]
    step_3.next_steps = [step_4.iri]
    step_4.next_steps = [step_5.iri]
    step_5.next_steps = [step_6.iri]

    demo_workflow_model.workflow_model_steps = {step.iri: step for step in [step_1, step_2, step_3, step_4, step_5, step_6]}
    demo_workflow_model.initial_step_iri = step_1.iri

    await store_workflow_model(demo_workflow_model)

    workflow_instance = WorkflowInstance()
    workflow_instance.create_new_iri()
    workflow_instance.name = "Correct workflow instance"
    workflow_instance.description = "A workflow instance whose assignments perfectly match its workflow model"
    workflow_instance.workflow_model_iri = demo_workflow_model.iri
    set_creator_user_id(workflow_instance, -1)

    workflow_assignment_1 = StepAssignment()
    workflow_assignment_1.create_new_iri()
    workflow_assignment_1.property_to_follow = crc_prefix.nextStep
    workflow_assignment_1.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_1.workflow_step_iri = step_1.iri

    workflow_assignment_2 = StepAssignment()
    workflow_assignment_2.create_new_iri()
    workflow_assignment_2.property_to_follow = crc_prefix.nextStep
    workflow_assignment_2.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_2.workflow_step_iri = step_2.iri

    workflow_assignment_3 = StepAssignment()
    workflow_assignment_3.create_new_iri()
    workflow_assignment_3.property_to_follow = crc_prefix.nextStep
    workflow_assignment_3.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_3.workflow_step_iri = step_3.iri

    workflow_assignment_4 = StepAssignment()
    workflow_assignment_4.create_new_iri()
    workflow_assignment_4.property_to_follow = crc_prefix.nextStep
    workflow_assignment_4.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_1"],
                                               crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_2"]]
    workflow_assignment_4.workflow_step_iri = step_4.iri

    workflow_assignment_5 = StepAssignment()
    workflow_assignment_5.create_new_iri()
    workflow_assignment_5.property_to_follow = crc_prefix.nextStep
    workflow_assignment_5.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_1"],
                                               crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_2"]]
    workflow_assignment_5.workflow_step_iri = step_5.iri

    workflow_assignment_6 = StepAssignment()
    workflow_assignment_6.create_new_iri()
    workflow_assignment_6.property_to_follow = crc_prefix.nextStep
    workflow_assignment_6.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_1"],
                                               crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_2"]]
    workflow_assignment_6.workflow_step_iri = step_6.iri

    workflow_instance.step_assignments = {sa.workflow_step_iri: sa for sa in [workflow_assignment_1, workflow_assignment_2,
                                                                              workflow_assignment_3, workflow_assignment_4,
                                                                              workflow_assignment_5, workflow_assignment_6]}

    await store_workflow_instance(workflow_instance)


async def create_demo_workflow_2():
    demo_workflow_model = WorkflowModel()
    demo_workflow_model.create_new_iri()
    demo_workflow_model.name = "Demo workflow 2"
    demo_workflow_model.description = "This is a branching demo workflow"
    demo_workflow_model.workflow_model_steps = {}
    set_creator_user_id(demo_workflow_model, -1)

    step_1 = CRC1625WorkflowModelStep()
    step_1.create_new_iri()
    step_1.name = "Step 1"
    step_1.description = "ML Synthesis and photo"
    step_1.set_allowed_activity_names(["Photo", "Report"])
    step_1.set_allowed_project_names(["A01"])

    step_2 = CRC1625WorkflowModelStep()
    step_2.create_new_iri()
    step_2.name = "Step 2"
    step_2.description = "Initial characterization (EDX and XRD)"
    step_2.set_allowed_activity_names(["EDX", "XRD"])
    step_2.set_allowed_project_names(["S"])

    step_3 = CRC1625WorkflowModelStep()
    step_3.create_new_iri()
    step_3.name = "Step 3"
    step_3.description = "Electrochemical screening"
    step_3.set_allowed_activity_names(["SECCM"])
    step_3.set_allowed_project_names(["C01"])

    step_4 = CRC1625WorkflowModelStep()
    step_4.create_new_iri()
    step_4.name = "Step 4"
    step_4.description = "Special sample synthesis and photo by A02"
    step_4.set_allowed_activity_names(["Photo", "Report"])
    step_4.set_allowed_project_names(["A02"])

    step_5 = CRC1625WorkflowModelStep()
    step_5.create_new_iri()
    step_5.name = "Step 5"
    step_5.description = "Initial characterization (EDX and XRD)"
    step_5.set_allowed_activity_names(["EDX", "XRD"])
    step_5.set_allowed_project_names(["S"])

    step_6_1 = CRC1625WorkflowModelStep()
    step_6_1.create_new_iri()
    step_6_1.name = "Step 6_1"
    step_6_1.description = "Electrochemical screening"
    step_6_1.set_allowed_activity_names(["SECCM"])
    step_6_1.set_allowed_project_names(["C01"])

    step_6_2 = CRC1625WorkflowModelStep()
    step_6_2.create_new_iri()
    step_6_2.name = "Step 6_2"
    step_6_2.description = "Incorrect EDX step (this step will fail because both samples went to C01 for electrochemical screening)"
    step_6_2.set_allowed_activity_names(["EDX"])
    step_6_2.set_allowed_project_names(["C01"])

    step_1.next_steps = [step_2.iri]
    step_2.next_steps = [step_3.iri]
    step_3.next_steps = [step_4.iri]
    step_4.next_steps = [step_5.iri]
    step_5.next_steps = [step_6_1.iri, step_6_2.iri]

    demo_workflow_model.workflow_model_steps = {step.iri: step for step in [step_1, step_2, step_3, step_4, step_5, step_6_1, step_6_2]}
    demo_workflow_model.initial_step_iri = step_1.iri

    await store_workflow_model(demo_workflow_model)

    workflow_instance = WorkflowInstance()
    workflow_instance.create_new_iri()
    workflow_instance.name = "Incorrect workflow instance"
    workflow_instance.description = "A workflow instance whose assignments don't match its workflow model at one of its steps"
    workflow_instance.workflow_model_iri = demo_workflow_model.iri
    set_creator_user_id(workflow_instance, -1)

    workflow_assignment_1 = StepAssignment()
    workflow_assignment_1.create_new_iri()
    workflow_assignment_1.property_to_follow = crc_prefix.nextStep
    workflow_assignment_1.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_1.workflow_step_iri = step_1.iri

    workflow_assignment_2 = StepAssignment()
    workflow_assignment_2.create_new_iri()
    workflow_assignment_2.property_to_follow = crc_prefix.nextStep
    workflow_assignment_2.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_2.workflow_step_iri = step_2.iri

    workflow_assignment_3 = StepAssignment()
    workflow_assignment_3.create_new_iri()
    workflow_assignment_3.property_to_follow = crc_prefix.nextStep
    workflow_assignment_3.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_3.workflow_step_iri = step_3.iri

    workflow_assignment_4 = StepAssignment()
    workflow_assignment_4.create_new_iri()
    workflow_assignment_4.property_to_follow = crc_prefix.nextStep
    workflow_assignment_4.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_1"],
                                               crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_2"]]
    workflow_assignment_4.workflow_step_iri = step_4.iri

    workflow_assignment_5 = StepAssignment()
    workflow_assignment_5.create_new_iri()
    workflow_assignment_5.property_to_follow = crc_prefix.nextStep
    workflow_assignment_5.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_1"],
                                               crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_2"]]
    workflow_assignment_5.workflow_step_iri = step_5.iri

    workflow_assignment_6_1 = StepAssignment()
    workflow_assignment_6_1.create_new_iri()
    workflow_assignment_6_1.property_to_follow = crc_prefix.nextStep
    workflow_assignment_6_1.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_1"]]
    workflow_assignment_6_1.workflow_step_iri = step_6_1.iri

    workflow_assignment_6_2 = StepAssignment()
    workflow_assignment_6_2.create_new_iri()
    workflow_assignment_6_2.property_to_follow = crc_prefix.nextStep
    workflow_assignment_6_2.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Sample_Piece_2"]]
    workflow_assignment_6_2.workflow_step_iri = step_6_2.iri

    workflow_instance.step_assignments = {sa.workflow_step_iri: sa for sa in [workflow_assignment_1, workflow_assignment_2,
                                                                              workflow_assignment_3, workflow_assignment_4,
                                                                              workflow_assignment_5, workflow_assignment_6_1,
                                                                              workflow_assignment_6_2]}

    await store_workflow_instance(workflow_instance)

    workflow_instance = WorkflowInstance()
    workflow_instance.create_new_iri()
    workflow_instance.name = "Workflow instance with missing data"
    workflow_instance.description = "A workflow instance whose assigned sample lacks data to validate the entire workflow"
    workflow_instance.workflow_model_iri = demo_workflow_model.iri
    set_creator_user_id(workflow_instance, -1)

    workflow_assignment_1 = StepAssignment()
    workflow_assignment_1.create_new_iri()
    workflow_assignment_1.property_to_follow = crc_prefix.nextStep
    workflow_assignment_1.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_1.workflow_step_iri = step_1.iri

    workflow_assignment_2 = StepAssignment()
    workflow_assignment_2.create_new_iri()
    workflow_assignment_2.property_to_follow = crc_prefix.nextStep
    workflow_assignment_2.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_2.workflow_step_iri = step_2.iri

    workflow_assignment_3 = StepAssignment()
    workflow_assignment_3.create_new_iri()
    workflow_assignment_3.property_to_follow = crc_prefix.nextStep
    workflow_assignment_3.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_3.workflow_step_iri = step_3.iri

    workflow_assignment_4 = StepAssignment()
    workflow_assignment_4.create_new_iri()
    workflow_assignment_4.property_to_follow = crc_prefix.nextStep
    workflow_assignment_4.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_4.workflow_step_iri = step_4.iri

    workflow_assignment_5 = StepAssignment()
    workflow_assignment_5.create_new_iri()
    workflow_assignment_5.property_to_follow = crc_prefix.nextStep
    workflow_assignment_5.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_5.workflow_step_iri = step_5.iri

    workflow_assignment_6_1 = StepAssignment()
    workflow_assignment_6_1.create_new_iri()
    workflow_assignment_6_1.property_to_follow = crc_prefix.nextStep
    workflow_assignment_6_1.assigned_entities = [crc_handover_prefix["hnd_group_initial_work_for_Demo_ML"]]
    workflow_assignment_6_1.workflow_step_iri = step_6_1.iri

    workflow_instance.step_assignments = {sa.workflow_step_iri: sa for sa in [workflow_assignment_1, workflow_assignment_2,
                                                                              workflow_assignment_3, workflow_assignment_4,
                                                                              workflow_assignment_5, workflow_assignment_6_1]}

    await store_workflow_instance(workflow_instance)


async def is_demo_data_already_loaded():
    result = await rdf_datastore_client.launch_query(are_there_workflow_models_from_demo_user_query)

    return result['boolean']

async def load_demo_user_data():
    await rdf_datastore_client.upload_file(os.path.join(module_dir, "assets/demo_data_graph.ttl"))

async def load_demo_workflows():
    await create_demo_workflow_1()
    await create_demo_workflow_2()