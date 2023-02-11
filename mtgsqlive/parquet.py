from pathlib import Path
import json
import itertools
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def execute(json_input: Path, output_file: Path) -> None:
    """Main function to handle the logic
    :param json_input: Input file (JSON)
    :param output_file: Output dir
    :param extras: additional json files to process
    """
    with json_input.open() as fp:
        parsed = json.load(fp)
    entities = list(parsed["data"].values())
    # # Pick only the first printing for each card ID
    # all_cards = itertools.chain.from_iterable((itertools.islice(set["cards"], 1) for set in sets))
    df = pd.DataFrame.from_records(entities)
    # Attach metadata as dictionary
    table = pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(parsed["meta"])
    pq.write_table(table, str(output_file))