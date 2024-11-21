"""
URL configuration for appreciate project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from testdb.views import CreateTableView,UploadClientData,UpdateTableData,DeleteTableData,SearchTableData
from django.http import HttpResponse



def home(request):
    return HttpResponse("Welcome to the Home Page!")


urlpatterns = [
    path('admin/', admin.site.urls),
    path('create_table/', CreateTableView.as_view(), name='create_table'),
    path('', home, name='home'),
    path("add_client_data/<str:table_name>/", UploadClientData.as_view(), name="add_client_data"),
    path("update_table_data/<str:table_name>/", UpdateTableData.as_view(), name="update_table_data"),
    path('delete_table_data/<str:table_name>/', DeleteTableData.as_view(), name='delete_table_data'),
    path('search_table_data/<str:table_name>/', SearchTableData.as_view(), name='search_table_data'),

]
