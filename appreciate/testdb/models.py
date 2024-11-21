from django.db import models

class DynamicTableSchema(models.Model):
    table_name = models.CharField(max_length=255)
    columns = models.JSONField()  
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.table_name




class TableSchema(models.Model):
    table_name = models.CharField(max_length=255, unique=True)
    schema = models.JSONField()  



class TableData(models.Model):
    table_name = models.ForeignKey(TableSchema, on_delete=models.CASCADE, related_name="data")
    data = models.JSONField()

    def __str__(self):
        return f"TableData object ({self.id})"
