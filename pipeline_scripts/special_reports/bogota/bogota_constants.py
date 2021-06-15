import os 

# Analysis
OBSERVATION_IDS = ["colombia_bogota_super_spreading_1",
                    "colombia_bogota_super_spreading_2",
                    "colombia_bogota_super_spreading_2",
                    "colombia_bogota_super_spreading_4",
                    "colombia_bogota_super_spreading_6",
                    "colombia_bogota_super_spreading_8"]

# Window to calculate "last week"
WINDOW = 6

# Date to use as baseline for "pre-pandemic" calculations
BASELINE_DATE = "2020-04-20"

# Values correspond to pandas.Weekday() -> 0: Monday, 6: Sunday
WEEKDAY_LIST = [0, 1, 2, 3]
WEEKEND_LIST = [4, 5, 6]

# Layout and design
COLORS = {'colombia_bogota_localidad_barrios_unidos': "#2D864A",
         'colombia_bogota_localidad_bosa': "#9D3434",
         'colombia_bogota_localidad_chapinero': "#DB9D94",
         'colombia_bogota_localidad_ciudad_bolivar': "#1D5058",
         'colombia_bogota_localidad_engativa': "#FFD700",
         'colombia_bogota_localidad_fontibon': "#A85038",
         'colombia_bogota_localidad_kennedy': "#C2475C",
         'colombia_bogota_localidad_los_martires': "#4FC4C2",
         'colombia_bogota_localidad_puente_aranda': "#BF7940",
         'colombia_bogota_localidad_rafael_uribe_uribe': "#B8953D",
         'colombia_bogota_localidad_san_cristobal': "#AFBB3E",
         'colombia_bogota_localidad_santa_fe': "#9FDFCA",
         'colombia_bogota_localidad_suba': "#3D6CB8",
         'colombia_bogota_localidad_teusaquillo': "#7F6ECF",
         'colombia_bogota_localidad_tunjuelito': "#453EBB",
         'colombia_bogota_localidad_usaquen': "#ACABE3",
         'colombia_bogota_localidad_usme': "#DB94C7",
         'colombia_bogota_localidad_antonio_narino': "#2B246B",
         'colombia_bogota_localidad_candelaria': "#6B2877"}

CMAP = 'tab20b'

MISSING_KWDS = {'color': 'lightgrey',
              "edgecolor": "black",
              "hatch": "///",
              "label": "Missing values"}

TRANSLATE = {'colombia_bogota_localidad_barrios_unidos': "Barrios Unidos",
             'colombia_bogota_localidad_bosa': "Bosa",
             'colombia_bogota_localidad_chapinero': "Chapinero",
             'colombia_bogota_localidad_ciudad_bolivar': "Ciudad Bolivar",
             'colombia_bogota_localidad_engativa': "Engativá",
             'colombia_bogota_localidad_fontibon': "Fontibón",
             'colombia_bogota_localidad_kennedy': "Kennedy",
             'colombia_bogota_localidad_los_martires': "Los Mártires",
             'colombia_bogota_localidad_puente_aranda': "Puente Aranda",
             'colombia_bogota_localidad_rafael_uribe_uribe': "Rafael Uribe Uribe",
             'colombia_bogota_localidad_san_cristobal': "San Cristobal",
             'colombia_bogota_localidad_santa_fe': "Santa Fé",
             'colombia_bogota_localidad_suba': "Suba",
             'colombia_bogota_localidad_teusaquillo': "Teusaquillo",
             'colombia_bogota_localidad_tunjuelito': "Tunjuelito",
             'colombia_bogota_localidad_usaquen': "Usaquén",
             'colombia_bogota_localidad_usme': "Usme",
             'colombia_bogota_localidad_antonio_narino': "Antonio Nariño",
             'colombia_bogota_localidad_candelaria': "La Candelaria",
             'colombia_bogota': 'Bogotá',
             'colombia_bogota_super_spreading_1': 'Teusaquillo - Clinica Palermo (2)',
             'colombia_bogota_super_spreading_2':'Engativa - Vivienda mixta (1)',
             'colombia_bogota_super_spreading_4': 'Suba - Parqueadero mulas (9)',
             'colombia_bogota_super_spreading_6': 'Kennedy Central - Plaza mercado (4)',
             'colombia_bogota_super_spreading_8': 'Los Cedros - Colegios privados (8)'
             }


bogota_scripts_path = "/".join(os.path.realpath(__file__).split("/")[:-1])
export_file = os.path.join(bogota_scripts_path, "export_locations.csv")