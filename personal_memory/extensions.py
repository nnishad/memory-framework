"""Producer schemas remain optional; configured namespaces are validated server-side."""
from .ingestion import ContractError


class ExtensionRegistry:
    def __init__(self,schemas=None):
        self.validators={}
        if schemas:
            from jsonschema import Draft202012Validator,FormatChecker
            def local_refs(value):
                if isinstance(value,dict):
                    for key in ("$ref","$dynamicRef"):
                        if key in value and (not isinstance(value[key],str) or not value[key].startswith("#")):
                            raise ValueError("Extension schemas may only reference their own document")
                    for child in value.values():local_refs(child)
                elif isinstance(value,list):
                    for child in value:local_refs(child)
            for namespace,versions in schemas.items():
                self.validators[namespace]={}
                for version,schema in versions.items():
                    local_refs(schema);Draft202012Validator.check_schema(schema)
                    self.validators[namespace][version]=Draft202012Validator(schema,format_checker=FormatChecker())

    def validate(self,records):
        for index,record in enumerate(records):
            for namespace,extension in record["extensions"].items():
                if namespace not in self.validators:continue
                version=extension["version"]
                path=f"$.items[{index}].extensions.{namespace}"
                if version not in self.validators[namespace]:raise ContractError(path+".version","Unsupported version of a registered extension")
                error=next(self.validators[namespace][version].iter_errors(extension["data"]),None)
                if error:
                    suffix="".join("."+str(p) for p in error.path)
                    raise ContractError(path+".data"+suffix,"Registered extension schema rejected this value")
