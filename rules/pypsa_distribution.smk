COSTS = "data/costs_dist.csv"
PROFILE = "data/dist_data/sample_profile.csv"

rule dist_ramp_build_demand_profile:
    params:
        ramp=config["ramp"],
        snapshoots=config["snapshots"],
    input:
        user_description="data/ramp/{user_type}.xlsx",
    output:
        daily_demand_profiles="dist_resources/ramp/daily_demand_{user_type}.xlsx",
        daily_type_demand_profile="dist_resources/ramp/daily_type_demand_{user_type}.xlsx",
    log:
        "logs/ramp_build_demand_profile_{user_type}.log",
    benchmark:
        "benchmarks/ramp_build_demand_profile_{user_type}"
    threads: 1
    resources:
        mem_mb=3000,
    script:
        "../scripts/dist_ramp_build_demand_profile.py"


rule dist_build_demand:
    params:
        tier=config["tier"],
        snapshots=config["snapshots"],
        build_demand_model=config["build_demand_type"],
    input:
        **{
            f"profile_{user_file.stem}": f"dist_resources/ramp/daily_type_demand_{user_file.stem}.xlsx"
            for user_file in Path("data/ramp/").glob("[a-zA-Z0-9]*.xlsx")
        },
        sample_profile=PROFILE,
        building_csv="dist_resources/buildings/buildings_type.csv",
        microgrid_shapes="dist_resources/shapes/microgrid_shapes.geojson",
        clusters_with_buildings="dist_resources/buildings/cluster_with_buildings.geojson",
    output:
        electric_load="dist_resources/demand/microgrid_load.csv",
    log:
        "logs/dist_build_demand.log",
    benchmark:
        "benchmarks/dist_build_demand"
    threads: 1
    resources:
        mem_mb=3000,
    script:
        "../scripts/dist_build_demand.py"


rule dist_build_shapes:
    params:
        countries=config["countries"],
    output:
        microgrid_shapes="dist_resources/shapes/microgrid_shapes.geojson",
        microgrid_bus_shapes="dist_resources/shapes/microgrid_bus_shapes.geojson",
    log:
        "logs/dist_build_shapes.log",
    benchmark:
        "benchmarks/dist_build_shapes"
    threads: 1
    resources:
        mem_mb=3000,
    script:
        "../scripts/dist_build_shapes.py"


if config.get("mode") != "brown_field":

    rule dist_cluster_buildings:
        params:
            crs=config["crs"],
            house_area_limit=config["house_area_limit"],
        input:
            buildings_geojson="dist_resources/buildings/microgrid_building.geojson",
        output:
            clusters="dist_resources/buildings/clustered_buildings.geojson",
            clusters_with_buildings="dist_resources/buildings/cluster_with_buildings.geojson",
            buildings_type="dist_resources/buildings/buildings_type.csv",
        log:
            "logs/dist_cluster_buildings.log",
        benchmark:
            "benchmarks/dist_cluster_buildings"
        threads: 1
        resources:
            mem_mb=3000,
        script:
            "../scripts/dist_cluster_buildings.py"

    rule dist_create_network:
        input:
            clusters="dist_resources/buildings/clustered_buildings.geojson",
            load="dist_resources/demand/microgrid_load.csv",
        output:
            "networks/" + RDIR + "base.nc",
        log:
            "logs/dist_create_network.log",
        benchmark:
            "benchmarks/dist_create_network"
        threads: 1
        resources:
            mem_mb=3000,
        script:
            "../scripts/dist_create_network.py"


if config["enable"].get("download_osm_buildings", True):

    rule dist_download_osm_data:
        output:
            buildings_resources="dist_resources/"
            + RDIR
            + "osm/raw/all_raw_buildings.geojson",
            lines_resources="dist_resources/" + RDIR + "osm/raw/all_raw_lines.geojson",
            cables_resources="dist_resources/" + RDIR + "osm/raw/all_raw_cables.geojson",
            generators_resources="dist_resources/"
            + RDIR
            + "osm/raw/all_raw_generators.geojson",
            substations_resources="dist_resources/"
            + RDIR
            + "osm/raw/all_raw_substations.geojson",
            poles_resources="dist_resources/" + RDIR + "osm/raw/all_raw_poles.geojson",
        log:
            "logs/" + RDIR + "dist_download_osm_data.log",
        benchmark:
            "benchmarks/" + RDIR + "dist_download_osm_data"
        threads: 1
        resources:
            mem_mb=3000,
        script:
            "../scripts/dist_download_osm_data.py"


rule dist_clean_earth_osm_data:
    input:
        all_buildings="dist_resources/" + RDIR + "osm/raw/all_raw_buildings.geojson",
        microgrid_shapes="dist_resources/shapes/microgrid_shapes.geojson",
    output:
        microgrid_building="dist_resources/buildings/microgrid_building.geojson",
    log:
        "logs/dist_clean_earth_osm_data.log",
    benchmark:
        "benchmarks/dist_clean_earth_osm_data"
    threads: 1
    resources:
        mem_mb=3000,
    script:
        "../scripts/dist_clean_earth_osm_data.py"


if config.get("mode") == "brown_field":

    rule dist_clean_osm_data:
        params:
            crs=config["crs"],
            clean_osm_data_options=config["clean_osm_data_options"],
        input:
            cables="dist_resources/" + RDIR + "osm/raw/all_raw_cables.geojson",
            generators="dist_resources/" + RDIR + "osm/raw/all_raw_generators.geojson",
            lines="dist_resources/" + RDIR + "osm/raw/all_raw_lines.geojson",
            substations="dist_resources/" + RDIR + "osm/raw/all_raw_substations.geojson",
            country_shapes="dist_resources/shapes/microgrid_shapes.geojson",
            offshore_shapes="resources/shapes/offshore_shapes.geojson",
            africa_shape="../dist_resources/shapes/africa_shape.geojson",
        output:
            generators="dist_resources/" + RDIR + "osm/clean/all_clean_generators.geojson",
            generators_csv="dist_resources/" + RDIR + "osm/clean/all_clean_generators.csv",
            lines="dist_resources/" + RDIR + "osm/clean/all_clean_lines.geojson",
            substations="dist_resources/" + RDIR + "osm/clean/all_clean_substations.geojson",
        log:
            "logs/" + RDIR + "clean_osm_data.log",
        benchmark:
            "benchmarks/" + RDIR + "clean_osm_data"
        script:
            "../scripts/clean_osm_data.py"

    rule dist_build_osm_network:
        params:
            build_osm_network=config.get("build_osm_network", {}),
            countries=config["countries"],
            crs=config["crs"],
        input:
            generators="dist_resources/" + RDIR + "osm/clean/all_clean_generators.geojson",
            lines="dist_resources/" + RDIR + "osm/clean/all_clean_lines.geojson",
            substations="dist_resources/" + RDIR + "osm/clean/all_clean_substations.geojson",
            country_shapes="dist_resources/" + RDIR + "shapes/microgrid_shapes.geojson",
        output:
            lines="dist_resources/" + RDIR + "base_network/all_lines_build_network.csv",
            converters="dist_resources/"
            + RDIR
            + "base_network/all_converters_build_network.csv",
            transformers="dist_resources/"
            + RDIR
            + "base_network/all_transformers_build_network.csv",
            substations="dist_resources/" + RDIR + "base_network/all_buses_build_network.csv",
        log:
            "logs/" + RDIR + "dist_build_osm_network.log",
        benchmark:
            "benchmarks/" + RDIR + "dist_build_osm_network"
        script:
            "../scripts/dist_build_osm_network.py"

    rule dist_cluster_buildings:
        params:
            crs=config["crs"],
            house_area_limit=config["house_area_limit"],
            voltage_node_cluster=config["electricity"]["voltage_node_cluster"],
        input:
            buildings_geojson="dist_resources/buildings/microgrid_building.geojson",
            all_nodes_brown_field="dist_resources/"
            + RDIR
            + "base_network/all_buses_build_network.csv",
        output:
            clusters="dist_resources/buildings/clustered_buildings.geojson",
            clusters_with_buildings="dist_resources/buildings/cluster_with_buildings.geojson",
            buildings_type="dist_resources/buildings/buildings_type.csv",
        log:
            "logs/dist_cluster_buildings.log",
        benchmark:
            "benchmarks/dist_cluster_buildings"
        threads: 1
        resources:
            mem_mb=3000,
        script:
            "../scripts/dist_cluster_buildings.py"

    rule dist_base_network:
        params:
            voltages=config["electricity"]["voltages"],
            transformers=config["transformers"],
            snapshots=config["snapshots"],
            links=config["links"],
            lines=config["lines"],
            hvdc_as_lines=config["electricity"]["hvdc_as_lines"],
            countries=config["countries"],
            base_network=config["base_network"],
        input:
            osm_buses="dist_resources/" + RDIR + "base_network/all_buses_build_network.csv",
            osm_lines="dist_resources/" + RDIR + "base_network/all_lines_build_network.csv",
            osm_converters="dist_resources/"
            + RDIR
            + "base_network/all_converters_build_network.csv",
            osm_transformers="dist_resources/"
            + RDIR
            + "base_network/all_transformers_build_network.csv",
            country_shapes="dist_resources/shapes/microgrid_shapes.geojson",
            offshore_shapes="resources/shapes/offshore_shapes.geojson",
        output:
            "networks/" + RDIR + "base.nc",
        log:
            "logs/" + RDIR + "base_network.log",
        benchmark:
            "benchmarks/" + RDIR + "base_network"
        threads: 1
        resources:
            mem_mb=500,
        script:
            "../scripts/base_network.py"

    rule dist_build_bus_regions:
        params:
            alternative_clustering=config["cluster_options"]["alternative_clustering"],
            crs=config["crs"],
            countries=config["countries"],
        input:
            country_shapes="dist_resources/shapes/microgrid_shapes.geojson",
            offshore_shapes="resources/shapes/offshore_shapes.geojson",
            base_network="networks/" + RDIR + "base.nc",
            #gadm_shapes="dist_resources/" + RDIR + "shapes/MAR2.geojson",
            #using this line instead of the following will test updated gadm shapes for MA.
            #To use: downlaod file from the google drive and place it in dist_resources/" + RDIR + "shapes/
            #Link: https://drive.google.com/drive/u/1/folders/1dkW1wKBWvSY4i-XEuQFFBj242p0VdUlM
            gadm_shapes="../dist_resources/" + RDIR + "shapes/gadm_shapes.geojson",
        output:
            regions_onshore="dist_resources/" + RDIR + "bus_regions/regions_onshore.geojson",
            regions_offshore="dist_resources/"
            + RDIR
            + "bus_regions/regions_offshore.geojson",
        log:
            "logs/" + RDIR + "build_bus_regions.log",
        benchmark:
            "benchmarks/" + RDIR + "build_bus_regions"
        threads: 1
        resources:
            mem_mb=1000,
        script:
            "../scripts/build_bus_regions.py"

    rule dist_filter_data:
        input:
            **{
                f"profile_{tech}": f"dist_resources/renewable_profiles/profile_{tech}.nc"
                for tech in config["tech_modelling"]["general_vre"]
            },
            base_network="networks/base.nc",
            raw_lines="dist_resources/osm/clean/all_clean_lines.geojson",
            shape="dist_resources/shapes/microgrid_shapes.geojson",
        output:
            base_update="networks/" + RDIR + "base_update.nc",
        log:
            "logs/" + RDIR + "dist_filter_data.log",
        benchmark:
            "benchmarks/" + RDIR + "dist_filter_data"
        threads: 1
        resources:
            mem_mb=500,
        script:
            "../scripts/dist_filter_data.py"


rule dist_build_renewable_profiles:
    params:
        crs=config["crs"],
        renewable=config["renewable"],
        countries=config["countries"],
        alternative_clustering=config["cluster_options"]["alternative_clustering"],
    input:
        natura="resources/natura.tiff",
        copernicus="data/copernicus/PROBAV_LC100_global_v3.0.1_2019-nrt_Discrete-Classification-map_EPSG-4326.tif",
        gebco="data/gebco/GEBCO_2025_sub_ice.nc",
        country_shapes="dist_resources/shapes/microgrid_shapes.geojson",
        offshore_shapes="resources/shapes/offshore_shapes.geojson",
        hydro_capacities="data/hydro_capacities.csv",
        eia_hydro_generation="data/eia_hydro_annual_generation.csv",
        powerplants="resources/powerplants.csv",
        regions=(
            (
                lambda w: (
                    ("dist_resources/" + RDIR + "bus_regions/regions_onshore.geojson")
                    if w.technology in ("onwind", "solar", "hydro", "csp")
                    else ("dist_resources/" + RDIR + "bus_regions/regions_offshore.geojson")
                )
            )
            if config.get("mode") == "brown_field"
            else "dist_resources/shapes/microgrid_bus_shapes.geojson"
        ),
        cutout=lambda w: "cutouts/" + config["renewable"][w.technology]["cutout"] + ".nc",
    output:
        profile="dist_resources/renewable_profiles/profile_{technology}.nc",
    log:
        "logs/build_renewable_profile_{technology}.log",
    benchmark:
        "benchmarks/build_renewable_profiles_{technology}"
    threads: ATLITE_NPROCESSES
    resources:
        mem_mb=ATLITE_NPROCESSES * 5000,
    script:
        "../scripts/build_renewable_profiles.py"


rule dist_add_electricity:
    params:
        mode=config["mode"],
    input:
        **{
            f"profile_{tech}": f"dist_resources/renewable_profiles/profile_{tech}.nc"
            for tech in config["tech_modelling"]["general_vre"]
        },
        create_network=(
            "networks/base_update.nc"
            if config.get("mode") == "brown_field"
            else "networks/base.nc"
        ),
        tech_costs=COSTS,
        load_file="dist_resources/demand/microgrid_load.csv",
        powerplants="dist_resources/powerplants.csv",
    output:
        "networks/elec.nc",
    log:
        "logs/dist_add_electricity.log",
    benchmark:
        "benchmarks/dist_add_electricity"
    threads: 1
    resources:
        mem_mb=3000,
    script:
        "../scripts/dist_add_electricity.py"


# if config["monte_carlo"]["options"].get("add_to_snakefile", False) == False:

# rule solve_network:
#     input:
#         "networks/elec.nc",
#     output:
#         "networks/results/elec.nc",
#     log:
#         "logs/solve_network.log",
#     benchmark:
#         "benchmarks/solve_network"
#     threads: 1
#     resources:
#         mem_mb=3000,
#     script:
#         "scripts/solve_network.py"


rule dist_solve_network:
    input:
        "networks/elec.nc",
    output:
        "networks/results/elec.nc",
    log:
        "logs/dist_solve_network.log",
    benchmark:
        "benchmarks/dist_solve_network"
    threads: 1
    resources:
        mem_mb=3000,
    script:
        "../scripts/dist_solve_network.py"
