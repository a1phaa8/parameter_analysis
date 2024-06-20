import json
import os

from data_handling.activity_code import *
from data_handling.cvreactivity_features_optimization import *
from data_handling.ehi_processing_functions import *

json_folder = "input/json_folder"


# Function to determine affect type for each element in the list
def determine_affect_type(affect_list):
    affect_type_list = []
    for affect in affect_list:
        if affect > 0:
            affect_type_list.append('positive')
        elif affect < 0:
            affect_type_list.append('negative')
        else:
            affect_type_list.append('neutral')
    return affect_type_list


def process_common_df(ehi_names, df_patient, cohort_all, common_params):
    min_date = ""
    max_date = ""
    processed_dataframes = {}
    sbp_flag, dbp_flag, hr_flag, hrv_flag, prq_flag, pa_flag, na_flag, cad_flag = 0, 0, 0, 0, 0, 0, 0, 0
    for ehi in ehi_names:
        if ehi == "":
            print("NULL\n")
            pass
        if ehi == 'cadphr-sbp' or ehi == 'cadphr-dbp':
            sbp_code = activity_code('resting', 'cadphr-sbp')
            dbp_code = activity_code('resting', 'cadphr-dbp')
            bp_json_file_path = os.path.join(json_folder, "cadphr-bloodpressure.json")
            with open(bp_json_file_path, 'r') as bp_json_file:
                bp_data = json.load(bp_json_file)
                bp_dataframe = pd.DataFrame(bp_data)
            if ehi == 'cadphr-sbp' and sbp_flag == 0:
                processed_sbp_df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    bp_dataframe, sbp_code, df_patient, cohort_all, *common_params, ehi)
                sbp_flag = 1
                processed_dataframes[ehi] = [processed_sbp_df, min_age, max_age, min_bmi, max_bmi, no_rows]

            elif ehi == 'cadphr-dbp' and dbp_flag == 0:
                processed_dbp_df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    bp_dataframe, dbp_code, df_patient, cohort_all, *common_params, ehi)
                dbp_flag = 1
                processed_dataframes[ehi] = [processed_dbp_df, min_age, max_age, min_bmi, max_bmi, no_rows]
        if ehi in 'cadphr-heartrate':
            if hr_flag == 0:
                hr_code = activity_code('resting', 'cadphr-heartrate')
                hr_json_file_path = os.path.join(json_folder, "cadphr-heartrate.json")
                with open(hr_json_file_path, 'r') as json_file:
                    hr_data = json.load(json_file)
                    hr_dataframe = pd.DataFrame(hr_data)
                processed_hr_df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(hr_dataframe,
                                                                                                           hr_code,
                                                                                                           df_patient,
                                                                                                           cohort_all,
                                                                                                           *common_params,
                                                                                                           ehi)
                processed_hr_df.rename(columns={'Latest': 'Latest_HR_Value'}, inplace=True)
                processed_dataframes[ehi] = [processed_hr_df, min_age, max_age, min_bmi, max_bmi, no_rows]
                hr_flag = 1
        if ehi in 'cadphr-hrv':
            if hrv_flag == 0:
                hrv_code = activity_code('resting', 'cadphr-hrv')
                hrv_json_file_path = os.path.join(json_folder, "cadphr-hrv.json")
                with open(hrv_json_file_path, 'r') as json_file:
                    hrv_data = json.load(json_file)
                    hrv_dataframe = pd.DataFrame(hrv_data)
                processed_hrv_df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    hrv_dataframe, hrv_code, df_patient, cohort_all, *common_params, ehi)
                processed_hrv_df.rename(columns={'Latest': 'Latest_HRV_Value'}, inplace=True)
                processed_dataframes[ehi] = [processed_hrv_df, min_age, max_age, min_bmi, max_bmi, no_rows]
                hrv_flag = 1
        if ehi in 'cadphr-prq':
            if prq_flag == 0:
                prq_code = activity_code('resting', 'cadphr-prq')
                prq_json_file_path = os.path.join(json_folder, "cadphr-prq.json")
                with open(prq_json_file_path, 'r') as json_file:
                    prq_data = json.load(json_file)
                    prq_dataframe = pd.DataFrame(prq_data)
                processed_prq_df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    prq_dataframe, prq_code, df_patient, cohort_all, *common_params, ehi)
                processed_prq_df.rename(columns={'Latest': 'Latest_PRQ_Value'}, inplace=True)
                processed_dataframes[ehi] = [processed_prq_df, min_age, max_age, min_bmi, max_bmi, no_rows]
                prq_flag = 1
        if ehi in 'cadphr-cadrisk10':
            if cad_flag == 0:
                json_file_path_cad = os.path.join(json_folder, f"{'cadphr-cadrisk10'}.json")
                with open(json_file_path_cad, 'r') as json_file:
                    ehi_data = json.load(json_file)
                    ehi_dataframe_cad = pd.DataFrame(ehi_data)
                processed_cad_df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    ehi_dataframe_cad, "", df_patient, cohort_all, *common_params, ehi)
                processed_dataframes[ehi] = [processed_cad_df, min_age, max_age, min_bmi, max_bmi, no_rows]
                cad_flag = 1
        if ehi in 'cadphr-vo2maxra':
            vo2maxra_code = activity_code('resting', 'cadphr-vo2maxra')
            ecrfra_code = activity_code('resting', 'cadphr-ecrfra')
            vo2maxra_json_file_path = os.path.join(json_folder, "cadphr-vo2maxra.json")
            with open(vo2maxra_json_file_path, 'r') as vo2maxra_json_file:
                vo2maxra_data = json.load(vo2maxra_json_file)
                vo2maxra_dataframe = pd.DataFrame(vo2maxra_data)

            ecrfra_json_file_path = os.path.join(json_folder, "cadphr-ecrfra.json")
            with open(ecrfra_json_file_path, 'r') as ecrfra_json_file:
                ecrfra_data = json.load(ecrfra_json_file)
                ecrfra_dataframe = pd.DataFrame(ecrfra_data)

            processed_vo2maxra_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                vo2maxra_dataframe, vo2maxra_code, df_patient, cohort_all, *common_params, ehi)

            processed_ecrfra_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                ecrfra_dataframe, ecrfra_code, df_patient, cohort_all, *common_params, "cadphr-ecrfra")
            processed_vo2maxra_dataframe.rename(columns={'Rolling AVG': 'Rolling AVG vo2max'}, inplace=True)
            processed_ecrfra_dataframe.rename(columns={'Rolling AVG': 'Rolling AVG ecrf'}, inplace=True)
            df_vo2max = processed_vo2maxra_dataframe[
                ['subject_reference', 'age', 'gender', 'BMI',
                 'Rolling AVG vo2max']]
            df_ecrf = processed_ecrfra_dataframe[['subject_reference', 'age', 'gender', 'BMI', 'Rolling AVG ecrf']]
            merged_df = pd.merge(df_vo2max, df_ecrf, on=['subject_reference', 'age', 'gender', 'BMI'], how='outer')
            merged_df = merged_df.rename(columns={'Rolling AVG vo2max': 'vo2max_obs'})
            merged_df['Rolling AVG'] = merged_df['vo2max_obs'].fillna(merged_df['Rolling AVG ecrf'])
            merged_df.drop(labels=['vo2max_obs', 'Rolling AVG ecrf'], axis=1, inplace=True)
            merged_df = add_grade_column(merged_df, ehi)
            merged_df['cadphr-vo2maxra_grade'] = merged_df['cadphr-vo2maxra_grade'].astype(int)
            processed_dataframes[ehi] = [merged_df, min_age, max_age, min_bmi, max_bmi, no_rows]
        if ehi in 'cadphr-hrrra':
            json_file_name = ehi
            json_file_path = os.path.join(json_folder, f"{json_file_name}.json")
            with open(json_file_path, 'r') as json_file:
                ehi_data = json.load(json_file)
                ehi_dataframe = pd.DataFrame(ehi_data)

            processed_hrrra_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                ehi_dataframe, "", df_patient, cohort_all, *common_params, ehi)
            processed_dataframes[ehi] = [processed_hrrra_dataframe, min_age, max_age, min_bmi, max_bmi, no_rows]

        if ehi in 'cadphr-osariskscore':
            json_file_path_osa = os.path.join(json_folder, f"{ehi}.json")

            with open(json_file_path_osa, 'r') as json_file:
                ehi_data = json.load(json_file)
                ehi_dataframe_osa = pd.DataFrame(ehi_data)

            processed_osa_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                ehi_dataframe_osa, "", df_patient, cohort_all, *common_params, ehi)
            processed_dataframes[ehi] = [processed_osa_dataframe, min_age, max_age, min_bmi, max_bmi, no_rows]

        if ehi in 'cadphr-diabetesriskscore':
            json_file_path_drs = os.path.join(json_folder, f"{ehi}.json")

            with open(json_file_path_drs, 'r') as json_file:
                ehi_data = json.load(json_file)
                ehi_dataframe_drs = pd.DataFrame(ehi_data)

            processed_drs_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                ehi_dataframe_drs, "", df_patient, cohort_all, *common_params, ehi)
            processed_dataframes[ehi] = [processed_drs_dataframe, min_age, max_age, min_bmi, max_bmi, no_rows]
        if ehi in ['cadphr-emotioninstability', 'cadphr-emotionvariability', 'cadphr-emotioninertia']:
            json_file_path_emotion = os.path.join(json_folder, f"{'cadphr-emotionmeasure'}.json")
            with open(json_file_path_emotion, 'r') as json_file:
                ehi_data = json.load(json_file)
                ehi_dataframe_ev = pd.DataFrame(ehi_data)
                ehi_dataframe_ei = pd.DataFrame(ehi_data)
                ehi_dataframe_einstability = pd.DataFrame(ehi_data)

            if ehi == 'cadphr-emotioninstability':
                processed_einstability_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    ehi_dataframe_einstability, "", df_patient, cohort_all, *common_params, ehi)
                processed_dataframes[ehi] = [processed_einstability_dataframe, min_age, max_age,
                                             min_bmi, max_bmi, no_rows]

            elif ehi == 'cadphr-emotioninertia':
                processed_ei_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    ehi_dataframe_ei, "", df_patient, cohort_all, *common_params, ehi)
                processed_dataframes[ehi] = [processed_ei_dataframe, min_age, max_age, min_bmi,
                                             max_bmi, no_rows]

            elif ehi == 'cadphr-emotionvariability':
                processed_ev_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    ehi_dataframe_ev, "", df_patient, cohort_all, *common_params, ehi)
                processed_dataframes[ehi] = [processed_ev_dataframe, min_age, max_age, min_bmi,
                                             max_bmi, no_rows]

        if ehi in 'cadphr-pa':
            if pa_flag == 0:
                json_file_path = os.path.join(json_folder, f"{ehi}.json")
                pa_code = activity_code('resting', 'cadphr-pa')
                with open(json_file_path, 'r') as json_file:
                    ehi_data = json.load(json_file)
                    ehi_dataframe_pa = pd.DataFrame(ehi_data)

                processed_pa_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    ehi_dataframe_pa, pa_code, df_patient, cohort_all, *common_params, ehi)

                processed_dataframes[ehi] = [processed_pa_dataframe, min_age, max_age, min_bmi,
                                             max_bmi, no_rows]
        if ehi in 'cadphr-na':
            if na_flag == 0:
                json_file_path = os.path.join(json_folder, f"{ehi}.json")
                na_code = activity_code('resting', 'cadphr-na')
                with open(json_file_path, 'r') as json_file:
                    ehi_data = json.load(json_file)
                    ehi_dataframe_na = pd.DataFrame(ehi_data)

                processed_na_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    ehi_dataframe_na, na_code, df_patient, cohort_all, *common_params, ehi)

                processed_dataframes[ehi] = [processed_na_dataframe, min_age, max_age, min_bmi,
                                             max_bmi, no_rows]

        if ehi == 'cadphr-dprp':
            json_file_path = os.path.join(json_folder, f"{ehi}.json")
            dprp_code = activity_code('resting', 'cadphr-dprp')
            with open(json_file_path, 'r') as json_file:
                ehi_data = json.load(json_file)
                ehi_dataframe_dprp = pd.DataFrame(ehi_data)

            processed_dprp_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                ehi_dataframe_dprp, dprp_code, df_patient, cohort_all, *common_params, ehi)
            processed_dataframes[ehi] = [processed_dprp_dataframe, min_age, max_age, min_bmi, max_bmi, no_rows]
        if ehi in 'cadphr-affect':
            ehi_pa = 'cadphr-pa'
            ehi_na = 'cadphr-na'
            if pa_flag == 0:
                json_file_path = os.path.join(json_folder, f"{ehi_pa}.json")
                pa_code = activity_code('resting', 'cadphr-pa')
                with open(json_file_path, 'r') as json_file:
                    ehi_data = json.load(json_file)
                    ehi_dataframe_pa = pd.DataFrame(ehi_data)

                processed_pa_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    ehi_dataframe_pa, pa_code, df_patient, cohort_all, *common_params, ehi_pa)
                pa_flag = 1
                processed_dataframes[ehi_pa] = [processed_pa_dataframe, min_age, max_age, min_bmi, max_bmi, no_rows]
            else:
                processed_pa_dataframe = processed_dataframes[ehi_pa][0]

            if na_flag == 0:
                json_file_path = os.path.join(json_folder, f"{ehi_na}.json")
                na_code = activity_code('resting', 'cadphr-na')
                with open(json_file_path, 'r') as json_file:
                    ehi_data = json.load(json_file)
                    ehi_dataframe_na = pd.DataFrame(ehi_data)

                processed_na_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    ehi_dataframe_na, na_code, df_patient, cohort_all, *common_params, ehi_na)
                na_flag = 1
                processed_dataframes[ehi_na] = [processed_na_dataframe, min_age, max_age, min_bmi, max_bmi, no_rows]
            else:
                processed_na_dataframe = processed_dataframes[ehi_na][0]

            columns_to_fill = ['PA', 'NA']
            merged_dataframe = pd.merge(processed_pa_dataframe[['subject_reference', 'PA', 'gender', 'age', 'BMI']],
                                        processed_na_dataframe[['subject_reference', 'NA', 'gender', 'age', 'BMI']],
                                        on=['subject_reference', 'gender', 'age', 'BMI'], how='outer')

            merged_dataframe[columns_to_fill] = merged_dataframe[columns_to_fill].fillna(0)
            merged_dataframe['affect'] = merged_dataframe['PA'] - merged_dataframe['NA']
            merged_dataframe['affect_type'] = merged_dataframe['affect'].apply(
                lambda x: 'positive' if x > 0 else 'negative' if x < 0 else 'neutral')
            merged_dataframe = affect_grades(merged_dataframe)
            processed_dataframes[ehi] = [merged_dataframe, min_age, max_age, min_bmi,
                                         max_bmi, no_rows]
        if ehi == 'cadphr-pulsepressure':
            ehi_sbp = 'cadphr-sbp'
            ehi_dbp = 'cadphr-dbp'
            sbp_code = activity_code('resting', 'cadphr-sbp')
            dbp_code = activity_code('resting', 'cadphr-dbp')
            bp_json_file_path = os.path.join(json_folder, "cadphr-bloodpressure.json")
            with open(bp_json_file_path, 'r') as bp_json_file:
                bp_data = json.load(bp_json_file)
                bp_dataframe = pd.DataFrame(bp_data)

            if sbp_flag == 0:
                processed_sbp_df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    bp_dataframe, sbp_code, df_patient, cohort_all, *common_params, ehi_sbp)
                sbp_flag = 1
                processed_dataframes[ehi_sbp] = [processed_sbp_df, min_age, max_age, min_bmi, max_bmi, no_rows]
            else:
                processed_sbp_df = processed_dataframes[ehi_sbp][0]

            if dbp_flag == 0:
                processed_dbp_df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process(
                    bp_dataframe, dbp_code, df_patient, cohort_all, *common_params, ehi_dbp)
                dbp_flag = 1
                processed_dataframes[ehi_dbp] = [processed_dbp_df, min_age, max_age, min_bmi, max_bmi, no_rows]
            else:
                processed_dbp_df = processed_dataframes[ehi_dbp][0]

            processed_dbp_df = processed_dbp_df[['subject_reference', 'DBP', 'gender', 'age']]
            processed_sbp_df = processed_sbp_df[['subject_reference', 'SBP', 'gender', 'age']]

            # Merge SBP and DBP DataFrames
            merged_dataframe = pd.merge(processed_sbp_df, processed_dbp_df,
                                        on=['subject_reference', 'age', 'gender'], how='outer')

            # Calculate 'Pulse Pressure'
            def subtract_lists(row):
                return [a - b for a, b in zip(row['SBP'], row['DBP'])]

            merged_dataframe['Pulse Pressure'] = merged_dataframe.apply(subtract_lists, axis=1)
            processed_pulsepressure_dataframe = process(merged_dataframe, "", df_patient, cohort_all, *common_params,
                                                        ehi)
            processed_dataframes[ehi] = [processed_pulsepressure_dataframe, min_age, max_age, min_bmi,
                                         max_bmi, no_rows]
        if ehi == 'cadphr-henergy':
            hrv_code = activity_code('resting', 'cadphr-hrv')

            heartrate_code = activity_code('resting', 'cadphr-heartrate')
            heartrate_json_file_path = os.path.join(json_folder, "cadphr-heartrate.json")

            with open(heartrate_json_file_path, 'r') as heartrate_json_file:
                heartrate_data = json.load(heartrate_json_file)
                df_heartrate = pd.DataFrame(heartrate_data)

            hrv_json_file_path = os.path.join(json_folder, "cadphr-hrv.json")
            with open(hrv_json_file_path, 'r') as hrv_json_file:
                hrv_data = json.load(hrv_json_file)
                df_hrv = pd.DataFrame(hrv_data)

            df_hr, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows  = extract_ehi_columns(df_heartrate, "cadphr-henergy", df_patient, cohort_all, heartrate_code, common_params[0],
                                        common_params[1], common_params[3], common_params[2])
            df_hrv, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = extract_ehi_columns(df_hrv, "cadphr-henergy", df_patient, cohort_all, hrv_code, common_params[0],
                                        common_params[1], common_params[3], common_params[2])
            df_heart_e = instabeats(df_hr, df_hrv, common_params[2])

            processed_dataframes[ehi] = [df_heart_e, min_age, max_age, min_bmi, max_bmi, no_rows]
        if ehi == 'cadphr-cvreactivity':
            no_rows = 0
            data = pd.read_json('data_handling/configuration.json', typ='series')
            cvreactivity_features = data["cvreactivity_features"]
            sbp_json_file_path = os.path.join(json_folder, "cadphr-bloodpressure.json")
            prq_json_file_path = os.path.join(json_folder, "cadphr-prq.json")
            heartrate_json_file_path = os.path.join(json_folder, "cadphr-heartrate.json")

            with open(sbp_json_file_path, 'r') as sbp_json_file:
                sbp_data = json.load(sbp_json_file)
                df_sbp = pd.DataFrame(sbp_data)

            with open(prq_json_file_path, 'r') as prq_json_file:
                prq_data = json.load(prq_json_file)
                df_prq = pd.DataFrame(prq_data)

            with open(heartrate_json_file_path, 'r') as heartrate_json_file:
                heartrate_data = json.load(heartrate_json_file)
                df_hr = pd.DataFrame(heartrate_data)

            print("cvreactivity functions start from here:")
            df_sbp = extract_sbp_new(df_sbp)
            df_prq = extract_new_ehi(df_prq, common_params[0], common_params[1])
            df_hr = extract_new_ehi(df_hr, common_params[0], common_params[1])

            min_date = min(df_hr['Date'])
            max_date = max(df_hr['Date'])
            min_age = min(df_patient['age'])
            max_age = max(df_patient['age'])
            min_bmi, max_bmi = min(df_patient['BMI']), max(df_patient['BMI'])

            # activity_codes
            # PRQ
            resting_prq = 'PHR-1001'
            postactivity_prq = 'PHR-1016'
            # HR
            resting_hr = '40443-4'
            postactivity_hr = '40442-6'
            # BP
            resting_bp = '85354-9'
            postactivity_bp = '88346-2'

            df_react_hr = reactivity_process_df(df_hr, 'cadphr-heartrate', resting_hr, postactivity_hr)
            df_react_sbp = reactivity_process_df(df_sbp, 'cadphr-bloodpressure', resting_bp, postactivity_bp)
            df_react_prq = reactivity_process_df(df_prq, 'cadphr-prq', resting_prq, postactivity_prq)
            react_features = ehi_input(cvreactivity_features)
            lst = lst_of_df_cvreact(cvreactivity_features, df_react_hr, df_react_sbp, df_react_prq)
            df_final = final_cv_reactivity(lst, react_features)
            df_final_copy = df_final
            df_final = pd.merge(df_final, df_patient, on='subject_reference', how='inner')
            print("Size after merging df: ", df_final.shape[0])
            if df_final.shape[0] == 0:
                no_rows = 1
                df_final = pd.merge(df_final_copy, cohort_all, on='subject_reference', how='inner')
                print("Size after merging df new: ", df_final.shape[0])
            processed_dataframes[ehi] = [df_final, min_age, max_age, min_bmi, max_bmi, no_rows]

    return processed_dataframes, min_date, max_date
