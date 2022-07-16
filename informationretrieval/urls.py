from django.urls import path

from informationretrieval import views_api

urlpatterns = [
    path('health/query_retrieval/', views_api.QueryRetrievalView.as_view()),
]