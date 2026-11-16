from django.urls import path

from . import views

urlpatterns = [
    path("", views.console, name="console"),
    path("run/<str:name>/", views.run_scenario_view, name="run_scenario"),
]
