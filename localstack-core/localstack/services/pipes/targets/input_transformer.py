import json
import re
from datetime import UTC, datetime

from localstack.services.lambda_.event_source_mapping.pipe_utils import to_json_str

PIPE_PLACEHOLDER_PATTERN = re.compile(r"<(.*?)>")


class PipeInputTransformer:
    """Handles InputTemplate with <$.jsonpath> and <aws.pipes.*> placeholder syntax for Pipes."""

    def __init__(
        self,
        input_template: str,
        pipe_arn: str,
        pipe_name: str,
        source_arn: str,
        target_arn: str,
    ):
        self.input_template = input_template
        self.pipe_arn = pipe_arn
        self.pipe_name = pipe_name
        self.source_arn = source_arn
        self.target_arn = target_arn

    def transform(self, event: dict) -> dict | str:
        """Apply the input template transformation to an event."""
        replacements = self._build_replacements(event)
        return self._replace_placeholders(self.input_template, replacements)

    def _build_replacements(self, event: dict) -> dict:
        """Build the replacement map from pipe variables and event jsonpath."""
        replacements = {
            "aws.pipes.pipe-arn": self.pipe_arn,
            "aws.pipes.pipe-name": self.pipe_name,
            "aws.pipes.source-arn": self.source_arn,
            "aws.pipes.target-arn": self.target_arn,
            "aws.pipes.event.ingestion-time": datetime.now(UTC).isoformat(),
            "aws.pipes.event.json": event,
            "aws.pipes.event": event,
        }

        # Extract jsonpath references from event
        placeholders = PIPE_PLACEHOLDER_PATTERN.findall(self.input_template)
        for placeholder in placeholders:
            if placeholder.startswith("$."):
                value = _extract_jsonpath(event, placeholder)
                replacements[placeholder] = value

        return replacements

    def _replace_placeholders(self, template: str, replacements: dict) -> dict | str:
        """Replace <placeholder> tokens in the template."""
        stripped = template.strip()

        # If the entire template is a single placeholder, return the value directly
        # to preserve its type (dict/list) rather than serializing to a string.
        single_match = PIPE_PLACEHOLDER_PATTERN.fullmatch(stripped)
        if single_match:
            value = replacements.get(single_match.group(1), "")
            if isinstance(value, (dict, list)):
                return value

        def replace_match(match):
            key = match.group(1)
            value = replacements.get(key, "")
            if isinstance(value, dict):
                return to_json_str(value)
            if isinstance(value, list):
                return json.dumps(value)
            if isinstance(value, bool):
                return json.dumps(value)
            return str(value)

        result = PIPE_PLACEHOLDER_PATTERN.sub(replace_match, template)

        if stripped.startswith("{"):
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                pass

        return result


def _extract_jsonpath(event: dict, path: str) -> any:
    """Simple jsonpath extraction supporting dot notation (e.g., $.body.key)."""
    if not path.startswith("$."):
        return ""
    keys = path[2:].split(".")
    current = event
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, "")
        else:
            return ""
    return current
