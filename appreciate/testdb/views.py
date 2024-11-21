from django.shortcuts import render
import pandas as pd
import json
from io import StringIO, BytesIO
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from .models import DynamicTableSchema,TableSchema, TableData
from .serializers import TableSchemaSerializer, DataValidator
from .utils import create_dynamic_table 
from django.db import connection,transaction
import logging,time


logger = logging.getLogger(__name__)




class CreateTableView(APIView):
    def post(self, request):
        serializer = TableSchemaSerializer(data=request.data)
        if serializer.is_valid():
            data = serializer.validated_data
            table_name = data['table_name']
            columns = data['columns']
            
            try:
                primary_keys = [col for col in columns if col.get('is_primary', False)]
                if not primary_keys:
                    return Response(
                        {"error": "Primary key must be explicitly defined."},
                        status=400,
                    )
                
                create_dynamic_table(table_name, columns)
                DynamicTableSchema.objects.create(table_name=table_name, columns=columns)
                return Response({"message": f"Table '{table_name}' created successfully."})
            except Exception as e:
                raise ValidationError(str(e))
        else:
            raise ValidationError(serializer.errors)





class UploadClientData(APIView):
    def post(self, request, table_name):
        try:
            print(f"Looking for table: {table_name}")

            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s;
                """, [table_name])

                table_exists = cursor.fetchone()
                
                if not table_exists:
                    return Response(
                        {"error": f"Table {table_name} not found in the database."},
                        status=status.HTTP_404_NOT_FOUND,
                    )

            # Fetch the table schema
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s;
                """, [table_name])

                columns = cursor.fetchall()
                print(f"Columns fetched: {columns}") 
            
                if not columns:
                    return Response(
                        {"error": f"No columns found for table {table_name}."},
                        status=status.HTTP_404_NOT_FOUND,
                    )
      
            schema = [{"column_name": col[0], "data_type": col[1]} for col in columns]
   

            # Parse the uploaded file
            uploaded_file = request.FILES.get("file")
            if not uploaded_file:
                return Response({"error": "File not provided."}, status=status.HTTP_400_BAD_REQUEST)

            file_extension = uploaded_file.name.split(".")[-1].lower()
            if file_extension == "json":
                client_data = json.load(uploaded_file)
            elif file_extension == "csv":
                df = pd.read_csv(StringIO(uploaded_file.read().decode("utf-8")))
                client_data = df.to_dict(orient="records")
            elif file_extension == "xlsx":
                df = pd.read_excel(BytesIO(uploaded_file.read()))
                client_data = df.to_dict(orient="records")
            else:
                return Response({"error": "Unsupported file format."}, status=status.HTTP_400_BAD_REQUEST)


            validator = DataValidator()
            validation_result = validator.validate_fields(client_data, schema)


            if not validation_result["is_valid"]:
                logger.error(f"Validation failed: {validation_result['invalid_transactions']}")
                return Response(
                    {
                        "msg": "Validation failed",
                        "invalid_transactions": validation_result["invalid_transactions"],
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            with connection.cursor() as cursor:
                for record in client_data:
                    placeholders = ', '.join(['%s'] * len(record))
                    column_names = ', '.join(record.keys())
                    insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
                    cursor.execute(insert_query, list(record.values()))

            return Response(
                {"msg": f"Data added successfully to table {table_name}", "inserted_count": len(client_data)},
                status=status.HTTP_201_CREATED,
            )

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)





class UpdateTableData(APIView):
    def put(self, request, table_name):
        try:

            primary_key_column = request.data.get("primary_key_column")
            primary_key_value = request.data.get("primary_key_value")
            update_data = request.data.get("update_data")

            if not primary_key_column or not primary_key_value or not update_data:
                return Response(
                    {
                        "error": "Missing required fields: 'primary_key_column', 'primary_key_value', or 'update_data'."
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )


            if not isinstance(update_data, dict):
                return Response(
                    {"error": "'update_data' must be a dictionary."},
                    status=status.HTTP_400_BAD_REQUEST,
                )


            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s;
                """, [table_name])
                table_exists = cursor.fetchone()

            if not table_exists:
                return Response(
                    {"error": f"Table {table_name} does not exist."},
                    status=status.HTTP_404_NOT_FOUND,
                )


            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s;
                """, [table_name])
                valid_columns = {row[0] for row in cursor.fetchall()}


            invalid_columns = [col for col in update_data.keys() if col not in valid_columns]
            if invalid_columns:
                return Response(
                    {"error": f"Invalid columns: {', '.join(invalid_columns)}."},
                    status=status.HTTP_400_BAD_REQUEST,
                )


            if primary_key_column not in valid_columns:
                return Response(
                    {"error": f"Primary key column '{primary_key_column}' does not exist in the table."},
                    status=status.HTTP_400_BAD_REQUEST,
                )


            set_clause = ", ".join(f"{key} = %s" for key in update_data.keys())
            query = f"""
                UPDATE {table_name}
                SET {set_clause}
                WHERE {primary_key_column} = %s;
            """
            query_values = list(update_data.values()) + [primary_key_value]

    
            with connection.cursor() as cursor:
                cursor.execute(query, query_values)

    
                if cursor.rowcount == 0:
                    return Response(
                        {"error": f"No row found with {primary_key_column} = {primary_key_value}."},
                        status=status.HTTP_404_NOT_FOUND,
                    )

            return Response(
                {"message": f"Record with {primary_key_column} = {primary_key_value} updated successfully."},
                status=status.HTTP_200_OK,
            )

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



class DeleteTableData(APIView):
    def delete(self, request, table_name):
        try:
         
            primary_key_column = request.data.get("primary_key_column")
            primary_key_value = request.data.get("primary_key_value")

            if not primary_key_column or not primary_key_value:
                return Response(
                    {
                        "error": "Missing required fields: 'primary_key_column' or 'primary_key_value'."
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )


            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s;
                """, [table_name])
                table_exists = cursor.fetchone()

            if not table_exists:
                return Response(
                    {"error": f"Table {table_name} does not exist."},
                    status=status.HTTP_404_NOT_FOUND,
                )


            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s;
                """, [table_name])
                valid_columns = {row[0] for row in cursor.fetchall()}

 
            if primary_key_column not in valid_columns:
                return Response(
                    {"error": f"Primary key column '{primary_key_column}' does not exist in the table."},
                    status=status.HTTP_400_BAD_REQUEST,
                )


            query = f"""
                DELETE FROM {table_name}
                WHERE {primary_key_column} = %s;
            """

            # Execute the delete query
            with connection.cursor() as cursor:
                cursor.execute(query, [primary_key_value])


                if cursor.rowcount == 0:
                    return Response(
                        {"error": f"No row found with {primary_key_column} = {primary_key_value}."},
                        status=status.HTTP_404_NOT_FOUND,
                    )

            return Response(
                {"message": f"Record with {primary_key_column} = {primary_key_value} deleted successfully."},
                status=status.HTTP_200_OK,
            )

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)






class SearchTableData(APIView):
    def post(self, request, table_name):
        try:
            search_params = request.data.get("search_params", {})
            order_by = request.data.get("order_by", None)
            order_direction = request.data.get("order_direction", "ASC").upper()

            if not search_params:
                return Response(
                    {"error": "Missing 'search_params' in request."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s;
                """, [table_name])
                table_exists = cursor.fetchone()

            if not table_exists:
                return Response(
                    {"error": f"Table {table_name} not found in the database."},
                    status=status.HTTP_404_NOT_FOUND,
                )

            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s;
                """, [table_name])
                columns = cursor.fetchall()

            if not columns:
                return Response(
                    {"error": f"No columns found for table {table_name}."},
                    status=status.HTTP_404_NOT_FOUND,
                )

            valid_columns = [col[0] for col in columns]

            invalid_columns = [col for col in search_params.keys() if col not in valid_columns]
            if invalid_columns:
                return Response(
                    {"error": f"Invalid column(s) in search params: {', '.join(invalid_columns)}."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            order_by_clause = ""
            if order_by:
                if order_by in valid_columns:
                    order_by_clause = f"ORDER BY {order_by} {order_direction}"
                else:
                    return Response(
                        {"error": f"Invalid column for ordering: {order_by}."},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

            where_clauses = [f"{key} = %s" for key in search_params.keys()]
            where_clause = " AND ".join(where_clauses)

            query = f"""
                SELECT * FROM {table_name}
                WHERE {where_clause}
                {order_by_clause};
            """
            query_values = list(search_params.values())

            with connection.cursor() as cursor:
                cursor.execute(query, query_values)
                rows = cursor.fetchall()

            if not rows:
                return Response(
                    {"message": "No records found matching the search criteria."},
                    status=status.HTTP_404_NOT_FOUND,
                )

            column_names = [col[0] for col in columns]
            results = [dict(zip(column_names, row)) for row in rows]

            return Response(
                {"results": results},
                status=status.HTTP_200_OK,
            )

        except Exception as e:
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )















# class UploadClientData(APIView):
#     def post(self, request, table_name):
#         try:
#             print(f"Looking for table: {table_name}")

#             # Verify if the table exists in the database
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT table_name
#                     FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s;
#                 """, [table_name])

#                 table_exists = cursor.fetchone()

#                 if not table_exists:
#                     return Response(
#                         {"error": f"Table {table_name} not found in the database."},
#                         status=status.HTTP_404_NOT_FOUND,
#                     )

#             # Now that the table exists, fetch its schema
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT column_name, data_type
#                     FROM information_schema.columns
#                     WHERE table_schema = 'public' AND table_name = %s;
#                 """, [table_name])

#                 columns = cursor.fetchall()

#                 if not columns:
#                     return Response(
#                         {"error": f"No columns found for table {table_name}."},
#                         status=status.HTTP_404_NOT_FOUND,
#                     )

#             # Prepare schema details for response
#             schema = [{"column_name": col[0], "data_type": col[1]} for col in columns]
#             print(schema)

#             table_schema_instance, created = TableSchema.objects.get_or_create(
#                 table_name=table_name,
#                 defaults={"schema": schema}
#             )

#             if not table_schema_instance.id:
#                 logger.error("Invalid table_schema_instance:", table_schema_instance)
#                 return Response({"error": "Invalid TableSchema reference."}, status=status.HTTP_400_BAD_REQUEST)

#             # Parse the uploaded file
#             uploaded_file = request.FILES.get("file")
#             if not uploaded_file:
#                 logger.error("File not provided.")
#                 return Response({"error": "File not provided."}, status=status.HTTP_400_BAD_REQUEST)

#             file_extension = uploaded_file.name.split(".")[-1].lower()
#             logger.info(f"File extension: {file_extension}")

#             client_data = None
#             if file_extension == "json":
#                 try:
#                     client_data = json.load(uploaded_file)
#                 except json.JSONDecodeError as e:
#                     logger.error(f"JSON decoding error: {str(e)}")
#                     return Response({"error": "Invalid JSON file."}, status=status.HTTP_400_BAD_REQUEST)
#             elif file_extension == "csv":
#                 try:
#                     df = pd.read_csv(StringIO(uploaded_file.read().decode("utf-8")))
#                     client_data = df.to_dict(orient="records")
#                 except Exception as e:
#                     logger.error(f"CSV reading error: {str(e)}")
#                     return Response({"error": "Invalid CSV file."}, status=status.HTTP_400_BAD_REQUEST)
#             elif file_extension == "xlsx":
#                 try:
#                     df = pd.read_excel(BytesIO(uploaded_file.read()))
#                     client_data = df.to_dict(orient="records")
#                 except Exception as e:
#                     logger.error(f"Excel reading error: {str(e)}")
#                     return Response({"error": "Invalid Excel file."}, status=status.HTTP_400_BAD_REQUEST)
#             else:
#                 logger.error("Unsupported file format.")
#                 return Response({"error": "Unsupported file format."}, status=status.HTTP_400_BAD_REQUEST)

#             # Validate the data
#             validator = DataValidator()
#             validation_result = validator.validate_fields(client_data, schema)

#             if not validation_result["is_valid"]:
#                 logger.error(f"Validation failed: {validation_result['invalid_transactions']}")
#                 return Response(
#                     {
#                         "msg": "Validation failed",
#                         "invalid_transactions": validation_result["invalid_transactions"],
#                     },
#                     status=status.HTTP_400_BAD_REQUEST,
#                 )

#             logger.info("Preparing to bulk create data...")

#             with transaction.atomic():
#                 start_time = time.time()
#                 print(client_data)
#                 inserted_rows = [
#                     TableData(
#                         table_name=table_schema_instance,
#                         data=row
#                     )
#                     for row in client_data
#                 ]
#                 print('debug')
               
#                 print(inserted_rows)
#                 inserted_count = TableData.objects.bulk_create(inserted_rows)
#                 for row in inserted_rows:
#                     print(row.id, row.table_name, row.data)
#                 end_time = time.time()
                

#                 if len(inserted_count) != len(client_data):
#                     logger.error(f"Number of inserted records ({len(inserted_count)}) does not match expected count ({len(client_data)})")
                
#                 logger.info(f"Transaction committed. Time taken: {end_time - start_time:.2f} seconds")

#             serialized_data = [{
#                 "id": data.id,
#                 "table_name": data.table_name.table_name,  # Fetch table_name from TableSchema model
#                 "data": data.data
#             } for data in inserted_rows]


#             return Response(
#                 {
#                     "msg": "Data added successfully",
#                     "inserted_count": len(inserted_count),
#                     "inserted_data": serialized_data,  # Send serialized data
#                 },
#                 status=status.HTTP_201_CREATED,
#             )

#         except TableSchema.DoesNotExist:
#             logger.error(f"Schema for table {table_name} not found in the system.")
#             return Response(
#                 {"error": f"Schema for table {table_name} not found in the system."},
#                 status=status.HTTP_404_NOT_FOUND,
#             )
#         except Exception as e:
#             logger.exception(f"An error occurred: {str(e)}")
#             return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



# class UploadClientData(APIView):
#     def post(self, request, table_name):
#         try:
#             print(f"Looking for table: {table_name}")

#             # Verify if the table exists in the database
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT table_name
#                     FROM information_schema.tables
#                     WHERE table_schema = 'public' AND table_name = %s;
#                 """, [table_name])

#                 table_exists = cursor.fetchone()
                
#                 if not table_exists:
#                     return Response(
#                         {"error": f"Table {table_name} not found in the database."},
#                         status=status.HTTP_404_NOT_FOUND,
#                     )

#             # Now that the table exists, fetch its schema
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT column_name, data_type
#                     FROM information_schema.columns
#                     WHERE table_schema = 'public' AND table_name = %s;
#                 """, [table_name])

#                 columns = cursor.fetchall()

#                 if not columns:
#                     return Response(
#                         {"error": f"No columns found for table {table_name}."},
#                         status=status.HTTP_404_NOT_FOUND,
#                     )

#             # Prepare schema details for response
#             schema = [{"column_name": col[0], "data_type": col[1]} for col in columns]
#             print(schema)

#             table_schema_instance, created = TableSchema.objects.get_or_create(
#                 table_name=table_name,
#                 defaults={"schema": schema}
#             )

#             if not table_schema_instance.id:
#                 print("Invalid table_schema_instance:", table_schema_instance)
#                 return Response({"error": "Invalid TableSchema reference."}, status=status.HTTP_400_BAD_REQUEST)


#             # Parse the uploaded file
#             uploaded_file = request.FILES.get("file")
#             if not uploaded_file:
#                 return Response({"error": "File not provided."}, status=status.HTTP_400_BAD_REQUEST)

#             file_extension = uploaded_file.name.split(".")[-1].lower()
#             if file_extension == "json":
#                 client_data = json.load(uploaded_file)
#             elif file_extension == "csv":
#                 df = pd.read_csv(StringIO(uploaded_file.read().decode("utf-8")))
#                 client_data = df.to_dict(orient="records")
#             elif file_extension == "xlsx":
#                 df = pd.read_excel(BytesIO(uploaded_file.read()))
#                 client_data = df.to_dict(orient="records")
#             else:
#                 return Response({"error": "Unsupported file format."}, status=status.HTTP_400_BAD_REQUEST)

#             # Validate the data
#             validator = DataValidator()
#             validation_result = validator.validate_fields(client_data, schema)

#             print(validation_result)

#             if not validation_result["is_valid"]:
#                 return Response(
#                     {
#                         "msg": "Validation failed",
#                         "invalid_transactions": validation_result["invalid_transactions"],
#                     },
#                     status=status.HTTP_400_BAD_REQUEST,
#                 )


#             print("Preparing to bulk create data...")

#             with transaction.atomic():
#                 TableData.objects.bulk_create([
#                     TableData(
#                         table_name=table_schema_instance,
#                         data=row
#                     )
#                     for row in client_data
#                 ])
#                 print("Transaction committed.")

#             # TableData.objects.bulk_create([
#             #     TableData(
#             #         table_name=table_schema_instance,
#             #         data=row
#             #     )
#             #     for row in client_data
#             # ])
#             # print("Bulk create executed successfully.")


#             return Response(
#                 {"msg": "Data added successfully", "inserted_count": len(client_data)},
#                 status=status.HTTP_201_CREATED,
#             )

#         except TableSchema.DoesNotExist:
#             return Response(
#                 {"error": f"Schema for table {table_name} not found in the system."},
#                 status=status.HTTP_404_NOT_FOUND,
#             )
#         except Exception as e:
#             return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)





