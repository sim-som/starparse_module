import pandas as pd
from pathlib import Path
import numpy as np

###################################################################################


def write_to_file(optics_data_str:str, particle_df:pd.DataFrame, output_file):
    # use newline="\n" to make text files compatible with linux/unix systems
    with open(output_file, mode="w", encoding="utf-8", newline="\n") as output:
        
        output.write(optics_data_str)

        part_data_header_str = f"# version 30001\n\ndata_particles\n\nloop_\n"
        for i, col_name in enumerate(particle_df.columns):
            part_data_header_str += f"{col_name} #{i+1}\n"


        output.write(part_data_header_str)

        particle_df.to_csv(output, header=None, index=None, sep='\t', mode='a', lineterminator="\n")
    

def parse_star_file(star_file_path: Path, keyword: str) -> pd.DataFrame:

    assert star_file_path.exists()
    assert star_file_path.is_file()

    def parse_column_names(star_file_path: str) -> list:
        parse_cols = False
        col_names = []

        at_keyword_position = False

        with open(star_file_path, mode="r", encoding="utf-8") as f:
            for row in f:

                # end column name parsing after last line starting with "_rln"
                if parse_cols and row[0:4] != "_rln":
                    break

                if row[0:4] == "_rln" and parse_cols and at_keyword_position:
                    col = row.split(" ")[0]
                    col = col[4:]
                    col_names.append(col)

                if row.strip() == "loop_" and at_keyword_position:
                    parse_cols = True
                    print("Parsing column names ...")

                if row.strip() == keyword:
                    print(f'Parsing "{keyword}"')
                    at_keyword_position = True

        print("Detected columns:", col_names)
        return col_names

    col_names = parse_column_names(star_file_path)


    parsing = False
    at_keyword_position = False
    data = []
    with open(star_file_path, mode="r", encoding="utf-8") as f:
        for row in f:
            if row.strip() == "":
                continue
        
            row_vals:list = row.split()
            
            # stop parsing when data block is finished.
            if parsing and row.strip() == "loop_":
                break
            
            # parse if all conditions are met:
            if parsing and at_keyword_position and len(row_vals) == len(col_names):
                # print(row_vals)
                row_dict = dict(zip(col_names, row_vals))
                data.append(row_dict)

            if row.strip() == "loop_" and at_keyword_position:
                parsing = True
                print("Parsing values ...")

            if row.strip() == keyword:
                at_keyword_position = True

    # generate DataFrame from list of dicts:
    data_df = pd.DataFrame(data)

    # parse / infer data types:
    data_df = data_df.apply(pd.to_numeric, errors="ignore")

    return data_df

##################################################################################
