from rest_framework import serializers

class TableSchemaSerializer(serializers.Serializer):
    table_name = serializers.CharField(max_length=255)
    columns = serializers.ListField(
        child=serializers.DictField(), allow_empty=False
    )



class DataValidator:
    
    def validate_fields(self, client_data, schema):
        print(schema)
        schema_dict = {col["column_name"]: col["data_type"] for col in schema}
        type_map = {
            "string": str,
            "integer": int,
            "bigint": int, 
            "float": float,
            "double precision": float,  
            "boolean": bool,
            "character varying": str, 
            "date": str,  
            "timestamp": str,  
            "array": list,
            "object": dict,
        }

        invalid_transactions = []
        for index, row in enumerate(client_data, start=1):
            errors = []
            for field, field_type in schema_dict.items():
                if field == "id":
                    continue 
                if field not in row:
                    errors.append(f"Missing field: {field}")
                elif not isinstance(row[field], type_map.get(field_type, object)):
                    errors.append(
                        f"Field '{field}' has invalid type. Expected {field_type}, got {type(row[field]).__name__}."
                    )

            if errors:
                invalid_transactions.append({"row": index, "entry": row, "errors": errors})

        return {
            "msg": "Validation completed",
            "invalid_transactions": invalid_transactions,
            "is_valid": len(invalid_transactions) == 0,
        }
