import yaml
import json
from google.cloud import bigquery
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def get_table_schema(client, table_id):
    try:
        table = client.get_table(table_id)
        return [field.name for field in table.schema]
    except Exception as e:
        print(f"❌ Errore nel recupero dello schema per '{table_id}': {e}")
        return []

def build_select_query(table_id, dataset, table_name, columns,  primary_key, placeholder, template_path="query_template.sql.j2" ):

    # Setup Jinja
    env = Environment(loader=FileSystemLoader(searchpath="."))
    template = env.get_template(template_path)

    query = template.render(
        table_id=table_id,
        dataset=dataset,
        table_name=table_name,
        columns=columns,
        primary_key=primary_key,
        ph=placeholder,
    )
    return query

def save_query_to_file(query, table_name, base_output_dir="advertising_linear_management_view"):
    # Estrai il nome della tabella (ultima parte del table_id)
    table_name = table_name
    view = f"view"

    # Costruisci il path completo: advertising_linear_management_view/<table_name>/view.sql
    output_dir = Path(base_output_dir) / table_name / view
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "view.sql"
    with open(output_file, "w") as f:
        f.write(query)

    print(f"✅ Query salvata in '{output_file}'")

def write_config_json(table_names, output_path="advertising_linear_management_view/config.json"):
    view_objects = []

    for table_name in table_names:
        view_objects.append({
            "project": "skyita-da-daita",
            "dataset_id": "advertising_linear_management_view",
            "view_name": table_name,
            "query_path": f"$CONFIG_PATH/flow/advertising_linear_management_view/{table_name}/view/view.sql",
            "source_projects": {
                "source_project_daita": "skyita-da-daita"
            }
        })

    config_data = {
        "view_objects": view_objects
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(config_data, f, indent=2)

    print(f"📝 Configurazione JSON scritta in '{output_path}'")



def parse_config(config_path="config.yaml"):
    config = load_config(config_path)

    proj_config = config.get("project", [])
    proj_entry = proj_config[0]
    tables_config = config.get("tables", [])
    proj = proj_entry.get("proj")
    ph = proj_entry.get("ph")

    valid_tables = []
    for entry in tables_config:
        dataset = entry.get("dataset")
        table_name = entry.get("table")
        primary_key = entry.get("primary_key")
        table_id = f"{proj}.{dataset}.{table_name}"
        ph = f"{ph}"

        valid_tables.append({
            "table_id": table_id,
            "primary_key": primary_key,
            "ph": ph,
            "dataset": dataset,
            "table_name": table_name
        })

    return valid_tables