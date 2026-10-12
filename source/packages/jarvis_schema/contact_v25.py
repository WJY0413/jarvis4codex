"""Thin adapter preserving the original V2.5 form and receiver semantics."""
from jarvis_schema import NAME, VERSION


def tool_spec(form):
    research = form.model_json_schema()
    definitions = research.pop("$defs", {})
    schema = {"type": "object", "properties": {"workspace": {"type": "string"}, "research": research},
              "required": ["workspace", "research"], "additionalProperties": False, "$defs": definitions}
    return {"type": "function", "name": "submit_contact_research", "deferLoading": False,
            "description": "Submit the current company's original V2.5 typed research form once. Workspace must equal the exact current task workspace. Receiver seals facts; it does not research or write production data.", "inputSchema": schema}


def validate_research(form, research):
    return form.model_validate(research).model_dump(mode="json", exclude_none=True)


def identity():
    return {"module": NAME, "version": VERSION, "form": "ContactResearchForm", "tool": "submit_contact_research",
            "receiver": "original v2.5/v24.submit_payload", "independent_accept": "original v24.accept", "business_fields_changed": False}
