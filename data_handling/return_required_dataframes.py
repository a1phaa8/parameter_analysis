from data_handling.optimized_dataframe_processing import *

data = pd.read_json('configuration.json', typ='series')
sliding_window = data["sliding_window"]
day_condition = data["day_condition"]
min_sufficiency = data["min_sufficiency"]
max_sufficiency = data["max_sufficiency"]


def process_data(ehi, start_age, end_age, start_date, end_date, BMI_lower_limit, BMI_upper_limit, data_select):
    ehi_dataframes = {}
    common_params = (start_date, end_date, sliding_window, day_condition, min_sufficiency, max_sufficiency)
    df_weight = pd.read_json('input/demographic_data/cadphr-bodyweight.json')
    df_height = pd.read_json('input/demographic_data/cadphr-bodyheight.json')

    df_patient, all_user_data, no_rows_patient = get_demographic_data(df_weight, df_height, data_select, start_age, end_age,
                                      BMI_lower_limit, BMI_upper_limit)

    processed_dataframes, min_date, max_date = process_common_df(ehi, df_patient, all_user_data, common_params)
    for ehi_value in ehi:
        processed_ehi_df = processed_dataframes[ehi_value][0]
        min_age, max_age = processed_dataframes[ehi_value][1], processed_dataframes[ehi_value][2]
        min_bmi, max_bmi = processed_dataframes[ehi_value][3], processed_dataframes[ehi_value][4]
        no_rows = processed_dataframes[ehi_value][5]
        ehi_dataframes[ehi_value] = processed_ehi_df
    no_rows_final = no_rows_patient or no_rows
    return ehi_dataframes, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows_final
