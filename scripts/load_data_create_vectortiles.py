# %%

import geopandas as gpd
import sqlalchemy
import os

# %%
exec(open("yaml_variables.py").read())

engine_info = (
    "postgresql://"
    + db_user
    + ":"
    + db_password
    + "@"
    + db_host
    + ":"
    + db_port
    + "/"
    + db_name
)

engine = sqlalchemy.create_engine(engine_info)
engine.connect()

# %%

# Load bikeability hexagons
bikeability_hex = gpd.read_postgis(
    "SELECT cluster_label, geometry FROM clustering.hex_clusters;",
    engine,
    geom_col="geometry",
)


# Load socio clusters
socio_clusters = gpd.read_postgis(
    "SELECT socio_label, geometry FROM clustering.socio_socio_clusters;",
    engine,
    geom_col="geometry",
)

# Load density hexagon data
hex_density = gpd.read_postgis(
    "SELECT ROUND(lts_1_dens::DECIMAL, 2) AS lts_1_dens, ROUND(lts_2_dens::DECIMAL, 2) AS lts_2_dens, ROUND(lts_3_dens::DECIMAL, 2) AS lts_3_dens, ROUND(lts_4_dens::DECIMAL, 2) AS lts_4_dens, ROUND(total_car_dens::DECIMAL, 2) AS car_dens, ROUND(total_network_dens::DECIMAL,2) AS total_network_dens, geometry FROM density.density_hex;",
    engine,
    geom_col="geometry",
)


# Load reach data
hex_reach = gpd.read_postgis(
    "SELECT ROUND(lts_1_reach_5,2) AS lts_1_reach_5, ROUND(lts_1_2_reach_5,2) AS lts_1_2_reach_5, ROUND(lts_1_3_reach_5,2) AS lts_1_3_reach_5, ROUND(lts_1_4_reach_5,2) AS lts_1_4_reach_5, ROUND(car_reach_5,2) AS car_reach_5, geometry FROM reach.compare_reach;",
    engine,
    geom_col="geometry",
)

# Load component data
# hex_largest_component = gpd.read_postgis(
#     "SELECT ROUND(component_length_1::DECIMAL,2) AS component_length_1, ROUND(component_length_1_2::DECIMAL,2) AS component_length_1_2, ROUND(component_length_1_3::DECIMAL,2) AS component_length_1_3, ROUND(component_length_1_4::DECIMAL,2) AS component_length_1_4, ROUND(component_length_car::DECIMAL,2), geometry FROM fragmentation.hex_largest_components;",
#     engine,
#     geom_col="geometry",
# )


# hex_largest_component.drop(
#     hex_largest_component[hex_largest_component.geometry.isnull()].index, inplace=True
# )

edges = gpd.read_postgis(
    "SELECT name, lts_access AS LTS, lts_viz AS type, geometry FROM edges;",
    engine,
    geom_col="geometry",
)

edges.fillna("unknown", inplace=True)

edges["type"] = edges["type"].replace("_", " ", regex=True)
# %%
for dataset in [
    bikeability_hex,
    socio_clusters,
    hex_density,
    hex_reach,
    # hex_largest_component,
]:
    dataset.fillna(0, inplace=True)

# %%
# Convert to geojson
to_json_names = [
    "bikeability_clusters",
    "socio_clusters",
    "density_lts_1",
    "density_lts_2",
    "density_lts_3",
    "density_lts_4",
    "reach_lts_1",
    "reach_lts_1_2",
    "reach_lts_1_3",
    "reach_lts_1_4",
    "edges",
]
to_json_data = [
    bikeability_hex,
    socio_clusters,
    hex_density[["lts_1_dens", "geometry"]],
    hex_density[["lts_2_dens", "geometry"]],
    hex_density[["lts_3_dens", "geometry"]],
    hex_density[["lts_4_dens", "geometry"]],
    hex_reach[["lts_1_reach_5", "geometry"]],
    hex_reach[["lts_1_2_reach_5", "geometry"]],
    hex_reach[["lts_1_3_reach_5", "geometry"]],
    hex_reach[["lts_1_4_reach_5", "geometry"]],
    edges,
]

if os.path.exists("../data/geojson") == False:
    os.mkdir("../data/geojson")

for name, dataset in zip(to_json_names, to_json_data):
    dataset.to_crs("EPSG:4326").to_file(f"../data/geojson/{name}.geojson")

print("Created geojsons!")

# %%
# Convert to vector tiles

if os.path.exists("../data/tiles") == False:
    os.mkdir("../data/tiles")

for name in to_json_names:

    fp = f"../data/tiles/tiles_{name}"

    if os.path.exists(fp) == False:

        os.mkdir(fp)

        dest = f"--output-to-directory=../data/tiles/tiles_{name}/"

        input_data = f"../data/geojson/{name}.geojson"

        # command = "tippecanoe -z16  --no-tile-compression" + " " + dest + " " + input_data
        # command = "tippecanoe -z16  --no-tile-compression" + " " + dest + " " + input_data

        command = (
            "tippecanoe -z15  --no-tile-compression --drop-densest-as-needed"
            + " "
            + dest
            + " "
            + input_data
        )
        print(command)

        os.system(command)

# %%

# EXPORT TO PARQUET

for dataset, name in zip(to_json_data, to_json_names):
    dataset.to_parquet(f"../data/parquet/{name}.parquet")

# %%
# Get bin values for density and reach data sets

density_columns = ["lts_1_dens", "lts_2_dens", "lts_3_dens", "lts_4_dens"]

dens_min = min(hex_density[density_columns].min())
dens_max = max(hex_density[density_columns].max())


# %%
reach_columns = [
    "lts_1_reach_5",
    "lts_1_2_reach_5",
    "lts_1_3_reach_5",
    "lts_1_4_reach_5",
]

reach_min = min(hex_reach[reach_columns].min())
reach_max = max(hex_reach[reach_columns].max())


# %%
import matplotlib as mpl
import numpy as np

cmap = mpl.colormaps["viridis"]

density_colors = [mpl.colors.rgb2hex(cmap(i)) for i in np.linspace(0, 1, 7)]

reach_colors = [mpl.colors.rgb2hex(cmap(i)) for i in np.linspace(0, 1, 9)]


# %%


# %%
