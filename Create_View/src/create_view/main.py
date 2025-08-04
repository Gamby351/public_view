# progetto/main.py
from google.cloud import bigquery
from utils import (
    parse_config,
    get_table_schema,
    build_select_query,
    save_query_to_file,
    write_config_json
)

def main():
    tables = parse_config()
    generated_table_names = []
    client = bigquery.Client()

    for entry in tables:
        table_id = entry["table_id"]
        primary_key = [key.strip() for key in entry["primary_key"].split(',')]
        ph = entry["ph"]
        dataset = entry["dataset"]
        table_name = entry["table_name"]

        print(f"🔍 Elaborazione tabella:{table_id} ")
        columns = get_table_schema(client, table_id)
        query = build_select_query(table_id, dataset, table_name, columns, primary_key, ph )

        if query:
            save_query_to_file(query, table_name)
            generated_table_names.append(table_name)
        else:
            print(f"⚠️ Query non generata per {table_id}")

    if generated_table_names:
        write_config_json(generated_table_names)
    else:
        print("⚠️ Nessuna query generata, file JSON non scritto.")

if __name__ == "__main__":
    main()
