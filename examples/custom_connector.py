"""A new source needs a connector, not a change to the memory service."""
from personal_memory.ingestion import ConnectorSpec,IngestionConnector


class NotesConnector(IngestionConnector):
    spec=ConnectorSpec(connector_id="example.notes",connector_version="1.0.0",source="personal-notes")

    def __init__(self, notes):
        self.notes=notes

    def read(self, checkpoint=None):
        for note in self.notes:
            yield {
                "schema_version":"1.0", "source":self.spec.source,
                "source_id":note["id"], "revision":note["revision"], "kind":"document",
                "occurred_at":note.get("created_at"), "observed_at":note["observed_at"],
                "text":note["text"], "participants":[],
                "provenance":{
                    "connector_id":self.spec.connector_id,"connector_version":self.spec.connector_version,
                    "source_locator":"notes://"+note["id"],"origin":"source","parent_record_ids":[]
                },
                "extensions":{
                    "example.notes":{"version":"1.0","data":{
                        "folder":note.get("folder"),"tags":note.get("tags",[]),
                        "custom_properties":note.get("custom_properties",{})
                    }}
                }
            }


# Delivery:
# from personal_memory.ingestion import submit
# for acknowledgment in submit(client, NotesConnector(notes)):
#     persist_source_checkpoint_after_ack(acknowledgment)
