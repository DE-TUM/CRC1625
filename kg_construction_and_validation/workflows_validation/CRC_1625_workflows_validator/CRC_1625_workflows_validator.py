import ast
import json
import os
from dataclasses import dataclass, asdict

from rdflib import URIRef, Namespace

from workflows_validation.common import dw_prefix, BaseWorkflowElement
from workflows_validation.workflows_validator import WorkflowModelStep

module_dir = os.path.dirname(__file__)
measurement_type_ids_to_activities: list[dict[str, list[str]]] = json.load(
    open(os.path.join(module_dir, "../../materialization/mappings/measurement_type_ids_to_activities.json")))
SHACL_shape = open(os.path.join(module_dir, "CRC_1625_handover_workflow_group_shape.shacl"), "r").read()

activity_name_to_class_iri = {str(m["measurement_name"]): dw_prefix[m["measurement_class_name"]] for m in measurement_type_ids_to_activities}

dw_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/")
crc_workflow_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/workflow/")
crc_project_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/project/")
crc_handover_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/handover/")
crc_sample_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/object/")
crc_user_prefix = Namespace("https://crc1625.mdi.ruhr-uni-bochum.de/user/")
pmdco_prefix = Namespace("https://w3id.org/pmd/co/")
rdf_prefix = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
rdfs_prefix = Namespace("http://www.w3.org/2000/01/rdf-schema#")
prov_prefix = Namespace("http://www.w3.org/ns/prov#")

project_name_to_iri = {
    "A01": URIRef(crc_project_prefix.A01),
    "A02": URIRef(crc_project_prefix.A02),
    "A03": URIRef(crc_project_prefix.A03),
    "A04": URIRef(crc_project_prefix.A04),
    "A05": URIRef(crc_project_prefix.A05),
    "A06": URIRef(crc_project_prefix.A06),

    "B01": URIRef(crc_project_prefix.B01),
    "B02": URIRef(crc_project_prefix.B02),
    "B03": URIRef(crc_project_prefix.B03),
    "B04": URIRef(crc_project_prefix.B04),
    "B05": URIRef(crc_project_prefix.B05),

    "C01": URIRef(crc_project_prefix.C01),
    "C02": URIRef(crc_project_prefix.C02),
    "C03": URIRef(crc_project_prefix.C03),
    "C04": URIRef(crc_project_prefix.C04),

    "INF": URIRef(crc_project_prefix.INF),
    "S": URIRef(crc_project_prefix.S),
    "Z": URIRef(crc_project_prefix.Z),
}

project_iri_to_name = {v: k for k, v in project_name_to_iri.items()}

allowed_activities = [dw_prefix[measurement_class_name] for measurement_class_name in activity_name_to_class_iri.values()]


@dataclass
class CRC1625WorkflowModelStep(WorkflowModelStep):
    """
    Helper class with methods for easily setting and retrieving the key-value replacements for CRC 1625 workflows

    This class is intended to be used by downcasting a WorkflowModelStep into a CRC1625WorkflowModelStep with the
    `from_step` method
    """
    _allowed_projects_key = "projects"
    _allowed_activities_key = "allowed_activities"
    _other_activities_allowed_key = "other_activities_allowed"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.SHACL_shape = SHACL_shape

    @classmethod
    def from_step(cls, parent_instance: WorkflowModelStep) -> 'CRC1625WorkflowModelStep':
        """
        Casts a WorkflowModelStep to a CRC1625WorkflowModelStep
        """
        parent_data = asdict(parent_instance)

        return cls(**parent_data)

    # Allowed projects
    def set_allowed_project_names(self, project_names: list[str]):
        # They are transformed into IRIs in the template
        self.step_templates[self._allowed_projects_key] = [str(project_name_to_iri[project_name]) for project_name in project_names]

    def get_allowed_project_names(self) -> list[str]:
        allowed_project_iris = self.step_templates.get(self._allowed_projects_key, [])
        if isinstance(allowed_project_iris, list):
            return [project_iri_to_name[URIRef(project_iri)] for project_iri in allowed_project_iris]
        else:
            return [project_iri_to_name[URIRef(allowed_project_iris)]]

    # Allowed activities
    def set_allowed_activity_names(self, activity_names: list[str]):
        # We format them for the template as activity_name|activity_class_iri
        activity_templates = [(activity_name, activity_name_to_class_iri[activity_name]) for activity_name in activity_names]
        self.step_templates[self._allowed_activities_key] = [f"{activity_name}|{activity_class_iri}" for activity_name, activity_class_iri in
                                                             activity_templates]

    def get_allowed_activity_names(self) -> list[str]:
        raw_activities = self.step_templates.get(self._allowed_activities_key, [])
        if isinstance(raw_activities, list):
            return [activity.split('|')[0] for activity in raw_activities]
        else:
            return [raw_activities.split('|')[0]]

    # other_activities_allowed flag
    def set_other_activities_allowed_flag(self, other_activities_allowed: bool):
        self.step_templates[self._other_activities_allowed_key] = str(other_activities_allowed)

    def are_other_activities_allowed(self) -> bool:
        if self._other_activities_allowed_key not in self.step_templates:
            return False
        else:
            return ast.literal_eval((self.step_templates[self._other_activities_allowed_key]))

    def get_base_step(self):
        workflow_model_step = WorkflowModelStep()
        workflow_model_step.iri = self.iri
        workflow_model_step.name = self.name
        workflow_model_step.description = self.description
        workflow_model_step.next_steps = self.next_steps
        workflow_model_step.step_templates = self.step_templates
        workflow_model_step.SHACL_shape = self.SHACL_shape
        workflow_model_step.provenance_records = self.provenance_records

        return workflow_model_step


# Common functions for all workflow elements


def get_creator_user_id(workflow_element: BaseWorkflowElement) -> int:
    creator_iri = dw_prefix.creator

    if creator_iri in workflow_element.provenance_records:
        user_id_str = str(workflow_element.provenance_records[creator_iri][0]).rsplit('/', 1)[-1]

        try:
            return int(user_id_str)
        except (ValueError, IndexError):
            return -1

    return -1


def set_creator_user_id(workflow_element: BaseWorkflowElement, user_id: int):
    creator_iri = dw_prefix.creator

    workflow_element.provenance_records[creator_iri] = [crc_user_prefix[str(user_id)]]
