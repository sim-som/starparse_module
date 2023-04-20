import pandas as pd
from pathlib import Path
import numpy as np

###################################################################################
# "Constants:"
DATATYPES_COLS = {
    "_rlnCoordinateX": float,
    "_rlnCoordinateY": float,
    "_rlnHelicalTubeID": float,
    "_rlnAngleTiltPrior": float,
    "_rlnAnglePsiPrior": float,
    "_rlnHelicalTrackLengthAngst": float,
    "_rlnAnglePsiFlipRatio": float,
    "_rlnCtfMaxResolution": float,
    "_rlnCtfFigureOfMerit": float,
    "_rlnDefocusU": float,
    "_rlnDefocusV": float,
    "_rlnDefocusAngle": float,
    "_rlnCtfBfactor": float,
    "_rlnCtfScalefactor": float,
    "_rlnPhaseShift": float,
    "_rlnMicrographOriginalPixelSize": float,
    "_rlnVoltage": float,
    "_rlnSphericalAberration": float,
    "_rlnAmplitudeContrast":float,
    "_rlnImagePixelSize": float,
    "_rlnImageSize": float,
    "_rlnImageDimensionality": float,
    "_rlnAngleRot": float,
    "_rlnAngleTilt": float,
    "_rlnAnglePsi": float,
    "_rlnOriginXAngst": float,
    "_rlnOriginYAngst": float,
    "_rlnNormCorrection": float,
    "_rlnLogLikeliContribution": float,
    "_rlnMaxValueProbDistribution": float,
    "_rlnClassDistribution": float,
    "_rlnAccuracyRotations": float,
    "_rlnAccuracyTranslationsAngst": float,
    "_rlnEstimatedResolution": float,
    "_rlnOverallFourierCompleteness": float,
    "_rlnClassPriorOffsetX": float,
    "_rlnClassPriorOffsetY": float,
    "_rlnGroupNumber": int,
    "_rlnHelicalTubeID": int,
    "_rlnClassNumber": int,
    "_rlnNrOfSignificantSamples": int,
    "_rlnImageName": str,
    "_rlnMicrographName": str,
    "_rlnOpticsGroup": str,
    "_rlnOpticsGroupName": str
}

###################################################################################

def reduce_mem_usage(df):
    """ iterate through all the columns of a dataframe and modify the data type
        to reduce memory usage.
        Source: https://www.kaggle.com/code/gemartin/load-data-reduce-memory-usage/notebook
    """
    start_mem = df.memory_usage().sum() / 1024**2
    print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))
    
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type != object and col_type != "category":
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        else:
            df[col] = df[col].astype('category')

    end_mem = df.memory_usage().sum() / 1024**2
    print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
    print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))
    
    return df



def parse_star_particle_data(star_file_path:str) -> pd.DataFrame:
    """
        TODO
    """
    def parse_column_names(star_file_path:str) -> list:
        parse_cols = False
        col_names = []

        at_particle_data_position = False

        with open(star_file_path, mode="r", encoding="utf-8") as f:
            for row in f:
                if row[0:4] == "_rln" and parse_cols and at_particle_data_position:
                    col = row.split(" ")[0]
                    col_names.append(col)


                if row.strip() == "loop_" and at_particle_data_position:
                    parse_cols = True
                    print("Parsing column names ...")
                
                if row.strip() == "data_particles":
                    print("data_particles")
                    at_particle_data_position = True
                

        print("Detected columns:", col_names)
        return col_names

    col_names = parse_column_names(star_file_path)

    parsing = False
    at_particle_data_position = False
    particle_data = []
    with open(star_file_path, mode="r", encoding="utf-8") as f:
        for row in f:
            if row.strip() == "":
                continue
            if parsing and row[0:4] != "_rln" and at_particle_data_position:
                row_vals:list = row.split()
                particle_dict = dict(zip(col_names, row_vals))

                # parse datatypes:
                for col_name, val in particle_dict.items():
                    try:
                        particle_dict[col_name] = DATATYPES_COLS[col_name](val)
                    except KeyError:
                        particle_dict[col_name] = str(val)

                particle_data.append(particle_dict)

            if row.strip() == "loop_" and at_particle_data_position:
                parsing = True
                print("Parsing values ...")
            
            if row.strip() == "data_particles":
                at_particle_data_position = True
            
            

    # generate DataFrame from list of dicts:
    particle_df = pd.DataFrame(particle_data)
    particle_df = particle_df.dropna()
    return particle_df

def parse_2dclasses_starfile(star_file_path):
    """
    Parse data about 2D classes from e.g. a model.star file from a Class2D-job or a class_averages.star file from a Select-job.
    """

    def parse_column_names(star_file_path:str) -> list:
        parse_cols = False
        col_names = []

        at_class_data_position = False

        with open(star_file_path, mode="r", encoding="utf-8") as f:
            for row in f:
                print(at_class_data_position, parse_cols)
                if row[0:4] == "_rln" and parse_cols and at_class_data_position:
                    print(row)
                    col = row.split(" ")[0]
                    col_names.append(col)
                
                if parse_cols and at_class_data_position and len(row.strip()) == 0:
                    break

                if row.strip() == "loop_" and at_class_data_position:
                    parse_cols = True
                    print("Parsing column names ...")
                
                if row.strip() in ["data_model_classes", "data_"]:
                    at_class_data_position = True
                    print(row)
                

        print("Detected columns:", col_names)
        return col_names

    col_names = parse_column_names(star_file_path)

    parsing = False
    at_particle_data_position = False
    particle_data = []
    with open(star_file_path, mode="r", encoding="utf-8") as f:
        for row in f:
            if row.strip() == "":
                continue
            if parsing and row[0:4] != "_rln" and at_particle_data_position:
                if len(row.strip()) == 0:
                    break
                row_vals:list = row.split()
                particle_dict = dict(zip(col_names, row_vals))

                # parse datatypes:
                for col_name, val in particle_dict.items():
                    try:
                        particle_dict[col_name] = DATATYPES_COLS[col_name](val)
                    except KeyError:
                        particle_dict[col_name] = str(val)

                particle_data.append(particle_dict)

            if row.strip() == "loop_" and at_particle_data_position:
                parsing = True
                print("Parsing values ...")
            
            if row.strip() == "data_":
                at_particle_data_position = True
            
            

    # generate DataFrame from list of dicts:
    particle_df = pd.DataFrame(particle_data)
    particle_df = particle_df.dropna()
    return particle_df



def parse_star_file(star_file_path, reduce_mem=True):
    
    particle_df = parse_star_particle_data(star_file_path)

    if reduce_mem:
        particle_df = reduce_mem_usage(particle_df)

    return particle_df

def parse_optics_metadata(star_file_path):
    metadata = []
    with open(star_file_path, mode="r", encoding="utf-8") as f:
        for row in f:
            # print(row)
            if row.strip() == "data_particles":
                break
            metadata.append(row)

    metadata.pop(-1)
    metadata.pop(-1)

    metadata_str = ""
    for item in metadata:
        metadata_str += f"{item}"

    return metadata_str

def write_to_file(optics_data_str:str, particle_df:pd.DataFrame, output_file):
    # use newline="\n" to make text files compatible with linux/unix systems
    with open(output_file, mode="w", encoding="utf-8", newline="\n") as output:
        
        output.write(optics_data_str)

        part_data_header_str = f"# version 30001\n\ndata_particles\n\nloop_\n"
        for i, col_name in enumerate(particle_df.columns):
            part_data_header_str += f"{col_name} #{i+1}\n"


        output.write(part_data_header_str)

        particle_df.to_csv(output, header=None, index=None, sep='\t', mode='a', lineterminator="\n")
    


##################################################################################

def main():

    teststring = "4"
    print(DATATYPES_COLS["_rlnAnglePsiPrior"](teststring))
    print(DATATYPES_COLS["_rlnHelicalTubeID"](teststring))
    print(DATATYPES_COLS["_rlnImageName"](teststring))

    test_file = "relion4_processing/Extract/job030/particles_small.star"

    part_df = parse_star_file(test_file, reduce_mem=True)
    print(part_df.head())

    print(part_df.info())

    mem_usage = part_df.memory_usage().sum() / 1024**2
    print(f"Memory usage: {mem_usage} MB")

    ##
    metadata = parse_optics_metadata(test_file)

    ##

    write_to_file(metadata, part_df, "test_output.star")

if __name__ == "__main__":
    main()