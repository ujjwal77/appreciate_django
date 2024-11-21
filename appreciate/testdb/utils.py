from django.db import connection, models

from django.db import connection, models

def create_dynamic_table(table_name, columns):
 
    attrs = {
        '__module__': __name__,  
        'Meta': type('Meta', (object,), {'db_table': table_name}),
    }

    primary_key_set = False

    for col in columns:
        column_name = col['column_name']
        field_type = col['data_type'].lower()
        is_nullable = col.get('is_nullable', True)
        is_unique = col.get('is_unique', False)
        default_value = col.get('default_value', None)
        is_primary = col.get('is_primary', False)

        if field_type == 'string':
            field = models.CharField(
                max_length=255,
                null=is_nullable,
                unique=is_unique,
                default=default_value,
            )
        elif field_type in ['int', 'integer']:
            field = models.IntegerField(
                null=is_nullable,
                unique=is_unique,
                default=default_value,
            )
        elif field_type == 'float':
            field = models.FloatField(
                null=is_nullable,
                unique=is_unique,
                default=default_value,
            )
        elif field_type == 'boolean':
            field = models.BooleanField(
                null=is_nullable,
                unique=is_unique,
                default=default_value,
            )
        elif field_type == 'date':
            field = models.DateField(
                null=is_nullable,
                unique=is_unique,
                default=default_value,
            )
        else:
            raise ValueError(f"Unsupported field type: {field_type}")
        
        if is_primary:
            if primary_key_set:
                raise ValueError("Only one column can be set as the primary key.")
            field.primary_key = True
            primary_key_set = True

        attrs[column_name] = field

    
    dynamic_model = type(table_name, (models.Model,), attrs)

    # Create the table in the database
    with connection.schema_editor() as schema_editor:
        schema_editor.create_model(dynamic_model)

    return dynamic_model


