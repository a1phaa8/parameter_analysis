import datetime
import warnings
from decimal import Decimal, ROUND_HALF_UP

from data_handling.ehi_Interpretation_functions import *
from data_handling.grading_functions import *
from data_handling.instaBeats_functions import *

warnings.filterwarnings("ignore")


# returning list of formatted EHI names
def get_name(ehi):
    if ehi in ['cadphr-emotioninstability']:
        formal_name = "Emotion Instability"
    elif ehi in ['cadphr-emotionvariability']:
        formal_name = "Emotion Variability"
    elif ehi in ['cadphr-emotioninertia']:
        formal_name = "Emotion Inertia"
    elif ehi in ['cadphr-emotionmeasure']:
        formal_name = "Emotion Measure"
    elif ehi in ['cadphr-cadrisk10']:
        formal_name = "CAD Risk 10"
    elif ehi in ['cadphr-heartrate']:
        formal_name = "Heart Rate"
    elif ehi in ['cadphr-hrv']:
        formal_name = "Heart Rate Variability"
    elif ehi in ['cadphr-hrrra']:
        formal_name = "Heart Rate Recovery"
    elif ehi in ['cadphr-osariskscore']:
        formal_name = "OSA Risk Score"
    elif ehi in ['cadphr-vo2maxra']:
        formal_name = "VO2 Max"
    elif ehi in ['cadphr-pa']:
        formal_name = "Positive Affect"
    elif ehi in ['cadphr-na']:
        formal_name = "Negative Affect"
    elif ehi in ['cadphr-henergy']:
        formal_name = "Heart Energy"
    elif ehi in ['cadphr-pulsepressure']:
        formal_name = "Pulse Pressure"
    elif ehi in ['cadphr-affect']:
        formal_name = "Affect"
    elif ehi in ['cadphr-sbp']:
        formal_name = "Systolic Blood Pressure"
    elif ehi in ['cadphr-dbp']:
        formal_name = "Diastolic Blood Pressure"
    elif ehi in ['cadphr-diabetesriskscore']:
        formal_name = "Diabetes Risk Score"
    elif ehi in ['cadphr-prq']:
        formal_name = "Pulse Respiration Quotient"
    elif ehi == 'cadphr-cvreactivity':
        formal_name = "CV Reactivity"
    elif ehi == 'cadphr-dprp':
        formal_name = "Double Product or Rate-Pressure Product"
    return formal_name


def extract_df_date_range(df, strt_d, end_d, min_d, max_d):
    if strt_d != "" and end_d != "":
        if (strt_d < min_d and end_d > max_d) or (strt_d < min_d and end_d < min_d) or (
                strt_d > max_d and end_d > max_d) or (strt_d > max_d and end_d < min_d):
            strt_d = min_d
            end_d = max_d
            df = df[df['Date'] >= strt_d]
            df = df[df['Date'] <= end_d]
            return df
        elif end_d > max_d:
            end_d = max_d
            df = df[df['Date'] >= strt_d]
            df = df[df['Date'] <= end_d]
            return df
        elif strt_d < min_d:
            strt_d = min_d
            df = df[df['Date'] >= strt_d]
            df = df[df['Date'] <= end_d]
            return df
        else:
            df = df[df['Date'] >= strt_d]
            df = df[df['Date'] <= end_d]
            return df
    elif strt_d != "":
        if strt_d < min_d or strt_d > max_d:
            strt_d = min_d
        end_d = max_d
        df = df[df['Date'] >= strt_d]
        df = df[df['Date'] <= end_d]
        return df
    elif end_d != "":
        if end_d > max_d or end_d < min_d:
            end_d = max_d
        strt_d = min_d
        df = df[df['Date'] >= strt_d]
        df = df[df['Date'] <= end_d]
        return df
    else:
        return df


# returning df with desired gender type
def data_pop(df, data_select):
    if data_select == "all" or data_select == "none":
        return df

    elif data_select == "male" or data_select == "female":
        df = df[df['gender'] == data_select]
    return df


# returning df with desired age range
def age_range(df, start_age, end_age):
    if start_age == "" and end_age == "":
        return df

    if start_age == "":
        end_age = int(end_age)
        df = df[df['age'] <= end_age]

    elif end_age == "":
        start_age = int(start_age)
        df = df[df['age'] >= start_age]
    else:
        start_age = int(start_age)
        end_age = int(end_age)
        df = df[(start_age <= df['age']) & (df['age'] <= end_age)]
    return df


def extract_ehi_columns(df, ehi_value, filtered_patient_dataframe, all_patient_data, act_code, start_date, end_date,
                        usr, wndw_sz):
    no_rows = 0
    df = df.dropna(subset=['code'])
    if ehi_value == 'cadphr-cvreactivity':
        df = df[['subject', 'effectiveDateTime', 'code', 'valueQuantity', 'encounter', 'resourceType']]
    else:
        df = df[['subject', 'effectiveDateTime', 'code', 'valueQuantity']]
    df = pd.concat([df.drop(['subject'], axis=1), df['subject'].apply(pd.Series)], axis=1)
    # inserting column at appropriate place
    ref = df['reference']
    df.drop(labels=['reference'], axis=1, inplace=True)
    df.insert(0, 'subject_reference', ref)

    df['effectiveDateTime'] = df['effectiveDateTime'].apply(lambda x: x[0:x.find('.')])
    df['effectiveDateTime'] = pd.to_datetime(df['effectiveDateTime'])
    df['effectiveDateTime'] = df['effectiveDateTime'].apply(lambda x: x.replace(microsecond=0, second=0))

    df_test = df['code'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['code', 'text'], axis=1, inplace=True)
    df_test = df['coding'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['coding'], axis=1, inplace=True)
    df_test = df[0].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['system', 0], axis=1, inplace=True)
    df.rename(columns={'code': 'activity_code'}, inplace=True)
    df = pd.concat([df, df['valueQuantity'].apply(pd.Series)], axis=1)
    # inserting column at appropriate place
    ref = df['value']
    df.insert(3, 'obs', ref)
    df.drop(labels=['value', 'valueQuantity', 'unit', 'system', 'code'], axis=1, inplace=True)

    df['subject_reference'] = df['subject_reference'].astype("str")
    df['subject_reference'] = df['subject_reference'].apply(
        lambda x: x.replace("Patient/", "", 1) if x.startswith("Patient/") else x)
    df = df[df.activity_code == act_code]

    ref_date = df['effectiveDateTime'].dt.date
    ref_time = df['effectiveDateTime'].dt.time

    df.insert(3, "Date", ref_date)
    df.insert(4, "Time", ref_time)
    date_min = min(df['Date'])
    date_max = max(df['Date'])
    df_copy = df
    print("EHI_COL")
    print("Size of df before date filtering:", df.shape)
    df = extract_df_date_range(df, start_date, end_date, date_min, date_max)
    print("Size of df after date filtering:", df.shape)
    df_date_copy = df
    if df.shape[0] == 0:
        no_rows = 1
        df = df_copy
    if ehi_value == 'cadphr-cvreactivity':
        df_encounter = df['encounter'].apply(pd.Series)
        df_encounter = df_encounter.rename(columns={'reference': 'encounter_reference'})
        df_encounter['encounter_reference'] = df_encounter['encounter_reference'].str.replace("Encounter/", "", 1)
        df = pd.concat([df, df_encounter], axis=1)
        df = df.drop(['encounter', 0], axis=1)
        df1 = df
        df = pd.merge(df, filtered_patient_dataframe)
        print("Size of df after merging:", df.shape)
        if df.shape[0] == 0:
            no_rows = 1
            df = pd.merge(df1, all_patient_data)
            print("Size of df after new merging:", df.shape)
        min_age, max_age = min(df['age']), max(df['age'])
        min_bmi, max_bmi = min(df['BMI']), max(df['BMI'])
        return df, date_min, date_max, min_age, max_age, min_bmi, max_bmi, no_rows
    elif ehi_value == 'cadphr-henergy':
        df = pd.merge(df, filtered_patient_dataframe)
        print("Size of df after merging:", df.shape)
        if df.shape[0] == 0:
            no_rows = 1
            df = pd.merge(df_date_copy, all_patient_data)
            print("Size of df after new merging:", df.shape)
        min_age, max_age = min(df['age']), max(df['age'])
        min_bmi, max_bmi = min(df['BMI']), max(df['BMI'])
        return df, date_min, date_max, min_age, max_age, min_bmi, max_bmi, no_rows

    round_lst = lambda lst: [round(x, 2) for x in lst]

    if usr == 'avg':
        # average
        avg_val = df.groupby(['subject_reference', 'Date'])['obs'].mean().reset_index()
        df = avg_val.groupby('subject_reference')['obs'].agg(list).reset_index()
        df['Rolling AVG'] = df['obs'].apply(lambda x: sliding_average(x, wndw_sz)).dropna()
        df = df[df['Rolling AVG'].apply(lambda x: len(x) > 0)]

        # If you want to reset the index of the resulting DataFrame
        df = df.reset_index(drop=True)

        # calculating average of rolling averages and assigning it to
        # Average column in the dataframe
        df['Latest'] = df['Rolling AVG'].apply(lambda x: x[-1] if x else None)
        # rounding off the values to 2 decimal places
        df['obs'] = df['obs'].apply(round_lst)
        df['Rolling AVG'] = df['Rolling AVG'].apply(round_lst)
    elif usr == 'latest':
        # latest
        df['effectiveDateTime'] = pd.to_datetime(df['effectiveDateTime'])
        latest_val = df.loc[df.groupby(['Date', 'subject_reference'])['effectiveDateTime'].idxmax()].reset_index()
        df = latest_val.groupby('subject_reference')['obs'].agg(list).reset_index()
        df = df[df['obs'].apply(lambda x: len(x) >= wndw_sz)]

        # Apply sliding average to the 'obs' column and create a new 'Rolling AVG' column
        df['Rolling AVG'] = df['obs'].apply(lambda x: sliding_average(x, wndw_sz))

        # Filter rows with non-empty 'Rolling AVG' lists
        df = df[df['Rolling AVG'].apply(lambda x: len(x) > 0)]

        # If you want to reset the index of the resulting DataFrame
        df = df.reset_index(drop=True)

        # calculating average of rolling averages and assigning it to
        # Average column in the dataframe
        df['Latest'] = df['Rolling AVG'].apply(lambda x: x[-1] if x else None)
        # rounding off the values to 2 decimal places
        df['obs'] = df['obs'].apply(round_lst)
        df['Rolling AVG'] = df['Rolling AVG'].apply(round_lst)
    df_roll_copy = df
    df = pd.merge(df, filtered_patient_dataframe)
    print("Size of df after merging:", df.shape)
    if df.shape[0] == 0:
        no_rows = 1
        df = pd.merge(df_roll_copy, all_patient_data)
        print("Size of df after new merging:", df.shape)
    min_age, max_age = min(df['age']), max(df['age'])
    min_bmi, max_bmi = min(df['BMI']), max(df['BMI'])
    return df, date_min, date_max, min_age, max_age, min_bmi, max_bmi, no_rows


def calculate_bmi(df_patient_height_weight):
    df_patient_height_weight['BMI'] = df_patient_height_weight['weight'] / np.power(
        df_patient_height_weight['height'] / 100, 2)
    return df_patient_height_weight


def BMI_range(df, start_BMI, end_BMI):
    if start_BMI == "" and end_BMI == "":
        return df
    if start_BMI == "":
        df = df[df['BMI'] <= end_BMI]
    if end_BMI == "":
        df = df[df['BMI'] >= start_BMI]
    if start_BMI != "" and end_BMI != "":
        df = df[(df['BMI'] > start_BMI) & (df['BMI'] <= end_BMI)]
    return df


def extract_height(df_all):
    df = df_all[['subject', 'valueQuantity', 'effectiveDateTime']]
    df = pd.concat([df.drop(['subject'], axis=1), df['subject'].apply(pd.Series)], axis=1)
    # inserting column at appropriate place
    ref = df['reference']
    df.drop(labels=['reference'], axis=1, inplace=True)
    df.insert(0, 'subject_reference', ref)
    df = pd.concat([df, df['valueQuantity'].apply(pd.Series)], axis=1)
    # inserting column at appropriate place
    ref = df['value']
    df.insert(3, 'height', ref)
    df.drop(labels=['value', 'valueQuantity', 'unit', 'system', 'code'], axis=1, inplace=True)
    df['subject_reference'] = df['subject_reference'].astype("str")
    df['subject_reference'] = df['subject_reference'].apply(
        lambda x: x.replace("Patient/", "", 1) if x.startswith("Patient/") else x)

    # Convert effectiveDateTime to datetime
    df['effectiveDateTime'] = pd.to_datetime(df['effectiveDateTime'])

    # Keep only unique and latest rows of each subject_reference
    df = df.sort_values(by=['subject_reference', 'effectiveDateTime'], ascending=[True, False])
    df = df.drop_duplicates(subset='subject_reference', keep='first')
    df.drop(labels=['effectiveDateTime'], axis=1, inplace=True)
    return df


def extract_weight(df_all):
    # Select relevant columns
    df = df_all[['subject', 'valueQuantity', 'effectiveDateTime']]

    # Split 'subject' column into subject_reference
    df = pd.concat([df.drop(['subject'], axis=1), df['subject'].apply(pd.Series)], axis=1)

    # Insert subject_reference column
    ref = df['reference']
    df.drop(labels=['reference'], axis=1, inplace=True)
    df.insert(0, 'subject_reference', ref)

    # Expand valueQuantity column
    df = pd.concat([df, df['valueQuantity'].apply(pd.Series)], axis=1)

    # Insert weight column
    ref = df['value']
    df.insert(3, 'weight', ref)

    # Drop unnecessary columns
    df.drop(labels=['value', 'valueQuantity', 'unit', 'system', 'code'], axis=1, inplace=True)

    # Convert subject_reference to string and remove prefix if present
    df['subject_reference'] = df['subject_reference'].astype(str)
    df['subject_reference'] = df['subject_reference'].apply(
        lambda x: x.replace("Patient/", "", 1) if x.startswith("Patient/") else x)

    # Convert effectiveDateTime to datetime
    df['effectiveDateTime'] = pd.to_datetime(df['effectiveDateTime'])

    # Keep only unique and latest rows of each subject_reference
    df = df.sort_values(by=['subject_reference', 'effectiveDateTime'], ascending=[True, False])
    df = df.drop_duplicates(subset='subject_reference', keep='first')
    df.drop(labels=['effectiveDateTime'], axis=1, inplace=True)

    return df


def get_demographic_data(user_data, weight_df, height_df, gender, start_age, end_age, start_BMI, end_BMI):
    no_rows = 0
    patient_df = pd.read_json(user_data)
    df_patient = extract_patient_info(patient_df, gender)
    df_copy = df_patient
    print("DEMOGRAPHICS")
    print("Size of df before age filtering:", df_patient.shape)
    df_patient = age_range(df_patient, start_age, end_age)
    print("Size of df after age filtering:", df_patient.shape)
    if df_patient.shape[0] == 0:
        df_patient = df_copy
        no_rows = 1
    df_height = extract_height(height_df)
    df_weight = extract_weight(weight_df)
    df_height_weight = pd.merge(df_height, df_weight, on='subject_reference', how='inner')
    df_bmi = calculate_bmi(df_height_weight)
    df_copy_bmi = df_bmi
    print("Size of df before bmi filtering:", df_bmi.shape)
    df_bmi = BMI_range(df_bmi, start_BMI, end_BMI)
    print("Size of df after bmi filtering:", df_bmi.shape)
    all_patient_data = pd.merge(df_copy, df_copy_bmi, on='subject_reference', how='inner')
    print("Size of original user data: ", all_patient_data.shape[0])
    if df_bmi.shape[0] == 0:
        no_rows = 1
        df_bmi = df_copy_bmi
    df_patient = pd.merge(df_patient, df_bmi, on='subject_reference', how='inner')
    if df_patient.shape[0] == 0:
        df_patient = df_copy
        no_rows = 1
        df_bmi = df_copy_bmi
        df_patient = pd.merge(df_patient, df_bmi, on='subject_reference', how='inner')

    print("Size of df after merging:", df_patient.shape)
    return df_patient, all_patient_data, no_rows


def extract_sbp_dbp_columns(df, ehi_value, filtered_patient_df, all_patient_data, start_date, end_date, usr, wndw_sz):
    no_rows = 0
    df = df[['subject', 'effectiveDateTime', 'code', 'encounter', 'component']]
    df = pd.concat([df.drop(['subject'], axis=1), df['subject'].apply(pd.Series)], axis=1)
    ref = df['reference']
    df.drop(labels=['reference'], axis=1, inplace=True)
    df.insert(0, 'subject_reference', ref)
    df['effectiveDateTime'] = df['effectiveDateTime'].apply(lambda x: x[0:x.find('.')])
    df['effectiveDateTime'] = pd.to_datetime(df['effectiveDateTime'])
    df_test = df['code'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['code', 'text'], axis=1, inplace=True)
    df_test = df['coding'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['coding'], axis=1, inplace=True)
    df_test = df[0].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=[0, 'system', 'display'], axis=1, inplace=True)
    df.rename(columns={'code': 'activity_code'}, inplace=True)
    df_test = df['encounter'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    df.drop(labels=['encounter', 0], axis=1, inplace=True)
    df.rename(columns={'reference': 'encounter_reference'}, inplace=True)
    df['subject_reference'] = df['subject_reference'].astype("str")
    df['subject_reference'] = df['subject_reference'].apply(
        lambda x: x.replace("Patient/", "", 1) if x.startswith("Patient/") else x)
    df['encounter_reference'] = df['encounter_reference'].astype("str")
    df['encounter_reference'] = df['encounter_reference'].apply(
        lambda x: x.replace("Encounter/", "", 1) if x.startswith("Encounter/") else x)
    df_test = df['component'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df[0]
    imp_col2 = df[1]
    df.drop(labels=[0, 1], axis=1, inplace=True)
    df.insert(0, 'component_1', imp_col2)
    df.insert(0, 'component_0', imp_col1)
    if ehi_value == "cadphr-sbp":
        df = extract_sbp(df)
    else:
        df = extract_dbp(df)
    ref_date = df['effectiveDateTime'].dt.date
    ref_time = df['effectiveDateTime'].dt.time
    df.insert(3, "Date", ref_date)
    df.insert(4, "Time", ref_time)
    min_date, max_date = min(df['Date']), max(df['Date'])
    df_copy = df
    print("SBP_DBP")
    print("Size of df before date filtering:", df.shape)
    df = extract_df_date_range(df, start_date, end_date, min_date, max_date)
    print("Size of df after date filtering:", df.shape)
    if df.shape[0] == 0:
        no_rows = 1
        df = df_copy
    round_lst = lambda lst: [round(x, 2) for x in lst]

    if usr == 'avg':
        # average
        avg_val = df.groupby(['subject_reference', 'Date'])['obs'].mean().reset_index()
        df = avg_val.groupby('subject_reference')['obs'].agg(list).reset_index()
        df['Rolling AVG'] = df['obs'].apply(lambda x: sliding_average(x, wndw_sz)).dropna()
        df = df[df['Rolling AVG'].apply(lambda x: len(x) > 0)]
        df = df.reset_index(drop=True)
        df['Latest'] = df['Rolling AVG'].apply(lambda x: x[-1] if x else None)
        # rounding off the values to 2 decimal places
        df['obs'] = df['obs'].apply(round_lst)
        df['Rolling AVG'] = df['Rolling AVG'].apply(round_lst)
    elif usr == 'latest':
        # latest
        df['effectiveDateTime'] = pd.to_datetime(df['effectiveDateTime'])
        latest_val = df.loc[df.groupby(['Date', 'subject_reference'])['effectiveDateTime'].idxmax()].reset_index()
        df = latest_val.groupby('subject_reference')['obs'].agg(list).reset_index()
        df = df[df['obs'].apply(lambda x: len(x) >= wndw_sz)]
        df['Rolling AVG'] = df['obs'].apply(lambda x: sliding_average(x, wndw_sz))
        df = df[df['Rolling AVG'].apply(lambda x: len(x) > 0)]
        df = df.reset_index(drop=True)
        df['Latest'] = df['Rolling AVG'].apply(lambda x: x[-1] if x else None)
        # rounding off the values to 2 decimal places
        df['obs'] = df['obs'].apply(round_lst)
        df['Rolling AVG'] = df['Rolling AVG'].apply(round_lst)
    df_roll_copy = df
    df = pd.merge(df, filtered_patient_df)
    print("Size of df after merging:", df.shape)
    if df.shape[0] == 0:
        no_rows = 1
        df = pd.merge(df_roll_copy, all_patient_data)
    min_age, max_age = min(df['age']), max(df['age'])
    min_bmi, max_bmi = min(df['BMI']), max(df['BMI'])
    return df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows


def extract_sbp(df1):
    #### Extracting SBP values
    # ======Extract values wrt component_0
    # =======================================================================================
    df_test = df1['component_0'].apply(pd.Series)
    df1 = pd.concat([df1, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df1['code']
    imp_col2 = df1['valueQuantity']
    df1.drop(labels=['code', 'valueQuantity'], axis=1, inplace=True)
    df1.insert(0, 'component_0_valueQuantity', imp_col2)
    df1.insert(0, 'component_0_code', imp_col1)

    # =======================================================================================
    df_test = df1['component_0_code'].apply(pd.Series)
    df1 = pd.concat([df1, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df1['coding']
    df1.drop(labels=['coding'], axis=1, inplace=True)
    df1.insert(0, 'component_0_code_coding', imp_col1)

    # =======================================================================================
    df_test = df1['component_0_code_coding'].apply(pd.Series)
    df1 = pd.concat([df1, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df1[0]
    df1.drop(labels=[0], axis=1, inplace=True)
    df1.insert(0, 'component_0_code_coding_0', imp_col1)

    # =======================================================================================
    df_test = df1['component_0_code_coding_0'].apply(pd.Series)
    df1 = pd.concat([df1, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df1['code']
    imp_col2 = df1['display']
    df1.drop(labels=['code'], axis=1, inplace=True)
    df1.insert(0, 'component_0_code_coding_0_code', imp_col1)
    df1.insert(0, 'component_0_code_coding_0_display', imp_col2)

    # =======================================================================================
    df_test = df1['component_0_valueQuantity'].apply(pd.Series)
    df1 = pd.concat([df1, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df1['unit']
    imp_col2 = df1['value']
    df1.drop(labels=['unit', 'value'], axis=1, inplace=True)
    df1.insert(0, 'component_0_valueQuantity_value', imp_col2)
    df1.insert(0, 'component_0_code_unit', imp_col1)

    # =======================================================================================
    df1 = df1[['subject_reference', 'effectiveDateTime', 'activity_code', 'encounter_reference',
               'component_0_valueQuantity_value']]
    df1.rename(columns={'component_0_valueQuantity_value': 'obs'}, inplace=True)
    return df1


def extract_dbp(df2):
    #### Extracting Diastolic Pressure
    # ======Extract values wrt component_1

    # =======================================================================================
    df_test = df2['component_1'].apply(pd.Series)
    df2 = pd.concat([df2, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df2['code']
    imp_col2 = df2['valueQuantity']
    df2.drop(labels=['code', 'valueQuantity'], axis=1, inplace=True)
    df2.insert(0, 'component_1_valueQuantity', imp_col2)
    df2.insert(0, 'component_1_code', imp_col1)

    # =======================================================================================
    df_test = df2['component_1_code'].apply(pd.Series)
    df2 = pd.concat([df2, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df2['coding']
    df2.drop(labels=['coding'], axis=1, inplace=True)
    df2.insert(0, 'component_1_code_coding', imp_col1)

    # =======================================================================================
    df_test = df2['component_1_code_coding'].apply(pd.Series)
    df2 = pd.concat([df2, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df2[0]
    df2.drop(labels=[0], axis=1, inplace=True)
    df2.insert(0, 'component_1_code_coding_0', imp_col1)

    # =======================================================================================
    df_test = df2['component_1_code_coding_0'].apply(pd.Series)
    df2 = pd.concat([df2, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df2['code']
    imp_col2 = df2['display']
    df2.drop(labels=['code'], axis=1, inplace=True)
    df2.insert(0, 'component_1_code_coding_0_code', imp_col1)
    df2.insert(0, 'component_1_code_coding_0_display', imp_col2)

    # =======================================================================================
    df_test = df2['component_1_valueQuantity'].apply(pd.Series)
    df2 = pd.concat([df2, df_test], axis=1)
    # inserting column at appropriate place
    imp_col1 = df2['unit']
    imp_col2 = df2['value']
    df2.drop(labels=['unit', 'value'], axis=1, inplace=True)
    df2.insert(0, 'component_1_valueQuantity_value', imp_col2)
    df2.insert(0, 'component_1_code_unit', imp_col1)

    # =======================================================================================
    df2 = df2[['subject_reference', 'effectiveDateTime', 'activity_code', 'encounter_reference',
               'component_1_valueQuantity_value']]

    df2.rename(columns={'component_1_valueQuantity_value': 'obs'}, inplace=True)
    return df2


def extract_observation_features(df, observation_extraction_features):
    df = df[observation_extraction_features]
    return df


def extract_patient_info(df, data_s):
    # extract observation features
    required_features = ['identifier', 'birthDate', 'gender']
    df = extract_observation_features(df, required_features)
    df = data_pop(df, data_s)
    # extract features
    df_test = df['identifier'].apply(pd.Series)
    df_test = df_test[0].apply(pd.Series)
    df = pd.concat([df.drop(['identifier'], axis=1), df_test], axis=1)
    # inserting column at appropriate place
    ind_val = df['value']
    df.drop(labels=['value'], axis=1, inplace=True)
    df.insert(1, 'identifier_value', ind_val)
    # ======================================================================
    df_info = df[['identifier_value', 'birthDate', 'gender']]
    df_info.rename(columns={"identifier_value": "subject_reference"}, inplace=True)
    # calculate age...................
    df_info['birthDate'] = df_info['birthDate'].fillna(-1)
    df_info['birthDate'] = df_info['birthDate'].astype(int)
    current_year = datetime.datetime.now().year
    df_info['age'] = df_info['birthDate'].apply(lambda x: current_year - x if x != -1 else -1)
    df_info['birthDate'] = df_info['birthDate'].astype(str)
    df_info['birthDate'] = df_info['birthDate'].replace('-1', np.nan)
    df_info['age'] = df_info['age'].astype(str)
    df_info['age'] = df_info['age'].replace('-1', np.nan)
    df_info = df_info.drop_duplicates()
    df_info.drop(labels=['birthDate'], axis=1, inplace=True)
    df_info['age'] = df_info['age'].astype(int)
    return df_info


def sliding_average(lst, window_size):
    return [sum(lst[i:i + window_size]) / window_size for i in range(len(lst) - window_size + 1)]


def min_max_scaling(lst, new_min=-1, new_max=1):
    if not lst or (max(lst) - min(lst)) == 0:
        return lst

    min_val = min(lst)
    max_val = max(lst)

    scaled_values = [(x - min_val) / (max_val - min_val) * (new_max - new_min) + new_min for x in lst]

    return scaled_values


def roll_avg_henergy(df, wndw_sz):
    round_lst = lambda lst: [round(x, 2) for x in lst]
    # Check if 'hr' and 'hrv' columns exist
    if 'hr' not in df.columns or 'hrv' not in df.columns:
        raise ValueError("Columns 'hr' and 'hrv' are required in the DataFrame.")
    # df=df[(df['hr'].apply(len) >= 3) & (df['hrv'].apply(len) >= 3)]
    # Calculate rolling average for 'hr' and 'hrv'
    df['hr_rolling_avg'] = df['hr'].apply(lambda x: sliding_average(x, wndw_sz)).reset_index(drop=True).dropna()
    df['hrv_rolling_avg'] = df['hrv'].apply(lambda x: sliding_average(x, wndw_sz)).reset_index(drop=True).dropna()
    df = df[(df['hr_rolling_avg'].apply(len) > 0) & (df['hrv_rolling_avg'].apply(len) > 0)]
    df['hr_rolling_avg'] = df['hr_rolling_avg'].apply(round_lst)
    df['hrv_rolling_avg'] = df['hrv_rolling_avg'].apply(round_lst)
    return df


# instaBEATS grades calculation for Heart Energy
def instabeats(df_hrv, df_heartrate, wndw_sz):
    df_hrv.rename(columns={'obs': 'hrv'}, inplace=True)
    df_heartrate.rename(columns={'obs': 'hr'}, inplace=True)
    df_hr_hrv = pd.merge(df_hrv, df_heartrate, on=["subject_reference", "effectiveDateTime", "age", "gender", "BMI"],
                         how="inner")
    df_hr_hrv = df_hr_hrv.sort_values(["subject_reference", "effectiveDateTime"])
    grouped = df_hr_hrv.groupby(['subject_reference'])
    df_agg = grouped[['hr', 'hrv']].agg(list).reset_index()
    df_agg['age'] = df_hr_hrv.groupby(['subject_reference'])['age'].first().reset_index()['age']
    df_agg['gender'] = df_hr_hrv.groupby(['subject_reference'])['gender'].first().reset_index()['gender']
    # df_agg['height_cm'] = df_hr_hrv.groupby(['subject_reference'])['height_cm'].first().reset_index()['height_cm']
    # df_agg['weight_kg'] = df_hr_hrv.groupby(['subject_reference'])['weight_kg'].first().reset_index()['weight_kg']
    df_agg['BMI'] = df_hr_hrv.groupby(['subject_reference'])['BMI'].first().reset_index()['BMI']
    df_agg['effectiveDateTime'] = df_hr_hrv.groupby(['subject_reference'])['effectiveDateTime'].first().reset_index()[
        'effectiveDateTime']

    df_real = df_agg.copy()
    df_real = df_real[['subject_reference', "effectiveDateTime", 'hr', 'hrv', 'age', 'gender', 'BMI']]
    df_real.rename(columns={'effectiveDateTime': 'TimeofRecord'}, inplace=True)
    df_real['age'] = df_real['age'].astype(int)
    df_real = roll_avg_henergy(df_real, 3)
    df_real['hrv_opt'] = df_real.apply(lambda x: get_optimal_HRV(x.age, x.gender), axis=1)
    df_real['hr_opt'] = df_real.apply(lambda x: get_optimal_HR(x.age, x.gender), axis=1)
    df_real = processing_instaBeats(df_real)
    df_real_latest = df_real.groupby(['subject_reference']).nth(0)
    df_real_latest = df_real_latest.reset_index()
    round_lst = lambda lst: [round(x, 2) for x in lst]
    df_real_latest['Rolling AVG'] = df_real_latest['InstaBEATS'].apply(lambda x: sliding_average(x, wndw_sz))
    # Filter rows with non-empty 'Rolling AVG' lists
    df_real_latest = df_real_latest[df_real_latest['Rolling AVG'].apply(lambda x: len(x) > 0)]
    # If you want to reset the index of the resulting DataFrame
    df_real_latest = df_real_latest.reset_index(drop=True)
    # calculating average of rolling averages and assigning it to
    # Average column in the dataframe
    df_real_latest['Average'] = df_real_latest['Rolling AVG'].apply(lambda x: calculate_average(x))
    # rounding off the values to 2 decimal places
    # df_real_latest['obs'] = df_real_latest['obs'].apply(round_lst)
    df_real_latest['Rolling AVG'] = df_real_latest['Rolling AVG'].apply(round_lst)
    df_real_latest['instabeats_value'] = df_real_latest['Rolling AVG'].apply(lambda x: x[-1] if x else None)
    df_real_latest['grade_list'] = df_real_latest.apply(henergy_grade, axis=1)
    df_real_latest['cadphr-henergy_grade'] = df_real_latest['grade_list'].apply(get_latest_grade)
    # df_real_latest = add_grade_column(df_real_latest, 'cadphr-henergy')
    return df_real_latest


def convert(input_value, input_range_start, input_range_end, output_range_start, output_range_end):
    if input_range_start == input_range_end:
        raise ValueError("input_range_start and input_range_end can't be equal")

    range_factor = (output_range_end - output_range_start) / (input_range_end - input_range_start)
    return ((input_value - input_range_start) * range_factor) + output_range_start


def compute_mssd_sliding_window(valences, window_size):
    if len(valences) < window_size:
        return None  # Return None if not enough data

    results = []
    for start in range(len(valences) - window_size + 1):
        window = valences[start:start + window_size]
        sum_ssd = sum((window[i + 1] - window[i]) ** 2 for i in range(len(window) - 1))
        mssd = sum_ssd / (2 * (len(window) - 1))
        input_range_end = (2 * len(window)) / (len(window) - 1)
        converted_mssd = convert(mssd, 0, input_range_end, 0, 1)

        # check the code previously to this
        mapped_cohen_value = (converted_mssd / input_range_end) * 2 - 1
        mapped_cohen_value = float(Decimal(mapped_cohen_value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        results.append((mssd, mapped_cohen_value))

    return results


def sliding_std(lst, window_size):
    if len(lst) < window_size:
        return np.nan  # Return NaN for lists with fewer than three elements
    return [np.std(lst[i:i + window_size]) for i in range(len(lst) - window_size + 1)]


def compute_autocorrelation_sliding_window(values, window_size):
    valences_count = len(values)
    if valences_count < window_size:
        return None  # Not enough data for even one window

    results = []
    for start in range(valences_count - window_size + 1):
        window = values[start:start + window_size]
        valence_mean = np.mean(window)
        valence_variance = np.var(window, ddof=0)

        sum_product = 0
        for i in range(len(window) - 1):
            sum_product += (window[i + 1] - valence_mean) * (window[i] - valence_mean)

        computed_value = 1.0  # Default to 1 if variance is zero
        if valence_variance != 0:
            computed_value = sum_product / valence_variance / (len(window) - 1)

        # Map and round computed autocorrelation value
        mapped_cohen_value = convert(computed_value, -1, 1, 0, 1)
        mapped_cohen_value = float(Decimal(mapped_cohen_value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        results.append((computed_value, mapped_cohen_value))

    return results


def extract_emotion_values(df):
    angle_values = []
    intensity_values = []
    valence_values = []

    for row in df['component']:
        if row:
            for ext in row[0]['extension']:
                if 'calculated-angle' in ext['url']:
                    angle_values.append(ext['valueDecimal'])
                elif 'calculated-intensity' in ext['url']:
                    intensity_values.append(ext['valueDecimal'])
                elif 'calculated-valence' in ext['url']:
                    valence_values.append(ext['valueDecimal'])

    if len(angle_values) > len(df):
        angle_values = angle_values[:len(df)]
    elif len(angle_values) < len(df):
        angle_values += [np.nan] * (len(df) - len(angle_values))

    if len(intensity_values) > len(df):
        intensity_values = intensity_values[:len(df)]
    elif len(intensity_values) < len(df):
        intensity_values += [np.nan] * (len(df) - len(intensity_values))

    if len(valence_values) > len(df):
        valence_values = valence_values[:len(df)]
    elif len(valence_values) < len(df):
        valence_values += [np.nan] * (len(df) - len(valence_values))

    df['angle'] = angle_values
    df['intensity'] = intensity_values
    df['valence'] = valence_values
    df = df.drop('component', axis=1)

    return df


def emotion_variability(df, filtered_patient_df, all_patient_data, min_sufficiency, sliding_window_size, start_date,
                        end_date):
    no_rows = 0
    df = df[['subject', 'effectiveDateTime', 'code', 'component']]

    df = pd.concat([df.drop(['subject'], axis=1), df['subject'].apply(pd.Series)], axis=1)
    ref = df['reference']
    df.drop(labels=['reference'], axis=1, inplace=True)
    df.insert(0, 'subject_reference', ref)
    df['effectiveDateTime'] = df['effectiveDateTime'].apply(lambda x: x[0:x.find('.')])
    df['effectiveDateTime'] = pd.to_datetime(df['effectiveDateTime'])
    df['effectiveDateTime'] = df['effectiveDateTime'].apply(lambda x: x.replace(microsecond=0, second=0))
    ref_date = df['effectiveDateTime'].dt.date
    ref_time = df['effectiveDateTime'].dt.time
    df.insert(3, "Date", ref_date)
    df.insert(4, "Time", ref_time)
    df_test = df['code'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    df.drop(labels=['code', 'text'], axis=1, inplace=True)
    df_test = df['coding'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['coding'], axis=1, inplace=True)
    df_test = df[0].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['system', 0], axis=1, inplace=True)
    df.rename(columns={'code': 'activity_code'}, inplace=True)
    date_min = min(df['Date'])
    date_max = max(df['Date'])
    df = extract_df_date_range(df, start_date, end_date, date_min, date_max)

    df = extract_emotion_values(df)
    df = df.dropna(subset=['angle', 'intensity', 'valence'])

    df['converted_valence'] = df['valence'].apply(lambda x: convert(x, -5, 5, -1, 1))
    avg_val = df.groupby(['subject_reference', 'effectiveDateTime'])[
        ['angle', 'intensity', 'valence', 'converted_valence']].mean().reset_index()
    df_result = avg_val.groupby('subject_reference')[['angle', 'intensity', 'valence', 'converted_valence']].agg(
        list).reset_index()
    df_result = df_result[
        (df_result['valence'].apply(len) >= min_sufficiency) & (df_result['angle'].apply(len) >= min_sufficiency) & (
                df_result['intensity'].apply(len) >= min_sufficiency)].reset_index(drop=True)
    df_result['angle'] = df_result['angle'].apply(lambda x: np.round(x, 2))
    df_result['intensity'] = df_result['intensity'].apply(lambda x: np.round(x, 2))
    df_result['valence'] = df_result['valence'].apply(lambda x: np.round(x, 2))
    df_result['valence'] = df_result['valence'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    df_result['converted_valence'] = df_result['converted_valence'].apply(lambda x: np.round(x, 2))
    df_result['converted_valence'] = df_result['converted_valence'].apply(
        lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    df_result['variability'] = df_result['converted_valence'].apply(lambda x: sliding_std(x, sliding_window_size))
    df_result_copy = df_result
    df_result = pd.merge(df_result, filtered_patient_df, on='subject_reference', how='inner')
    if df_result.shape[0] == 0:
        no_rows = 1
        df_result = pd.merge(df_result_copy, all_patient_data)
    min_age = min(df_result['age'])
    max_age = max(df_result['age'])
    min_bmi = min(df_result['BMI'])
    max_bmi = max(df_result['BMI'])
    return df_result, date_min, date_max, min_age, max_age, min_bmi, max_bmi, no_rows


def emotion_instability(df, filtered_patient_df, all_patient_data, min_sufficiency, sliding_window_size, start_date,
                        end_date):
    no_rows = 0
    df = df[['subject', 'effectiveDateTime', 'code', 'component']]

    df = pd.concat([df.drop(['subject'], axis=1), df['subject'].apply(pd.Series)], axis=1)
    ref = df['reference']
    df.drop(labels=['reference'], axis=1, inplace=True)
    df.insert(0, 'subject_reference', ref)
    df['effectiveDateTime'] = df['effectiveDateTime'].apply(lambda x: x[0:x.find('.')])
    df['effectiveDateTime'] = pd.to_datetime(df['effectiveDateTime'])
    df['effectiveDateTime'] = df['effectiveDateTime'].apply(lambda x: x.replace(microsecond=0, second=0))
    ref_date = df['effectiveDateTime'].dt.date
    ref_time = df['effectiveDateTime'].dt.time
    df.insert(3, "Date", ref_date)
    df.insert(4, "Time", ref_time)
    df_test = df['code'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    df.drop(labels=['code', 'text'], axis=1, inplace=True)
    df_test = df['coding'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['coding'], axis=1, inplace=True)
    df_test = df[0].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['system', 0], axis=1, inplace=True)
    df.rename(columns={'code': 'activity_code'}, inplace=True)
    date_min = min(df['Date'])
    date_max = max(df['Date'])
    df = extract_df_date_range(df, start_date, end_date, date_min, date_max)
    df = extract_emotion_values(df)
    df = df.dropna(subset=['angle', 'intensity', 'valence'])

    df['converted_valence'] = df['valence'].apply(lambda x: convert(x, -5, 5, -1, 1))
    avg_val = df.groupby(['subject_reference', 'effectiveDateTime'])[
        ['angle', 'intensity', 'valence', 'converted_valence']].mean().reset_index()
    df_result = avg_val.groupby('subject_reference')[['angle', 'intensity', 'valence', 'converted_valence']].agg(
        list).reset_index()
    df_result = df_result[
        (df_result['valence'].apply(len) >= min_sufficiency) & (df_result['angle'].apply(len) >= min_sufficiency) & (
                df_result['intensity'].apply(len) >= min_sufficiency)].reset_index(drop=True)
    df_result['angle'] = df_result['angle'].apply(lambda x: np.round(x, 2))
    df_result['intensity'] = df_result['intensity'].apply(lambda x: np.round(x, 2))
    df_result['valence'] = df_result['valence'].apply(lambda x: np.round(x, 2))
    df_result['valence'] = df_result['valence'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    df_result['converted_valence'] = df_result['converted_valence'].apply(lambda x: np.round(x, 2))
    df_result['converted_valence'] = df_result['converted_valence'].apply(
        lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    results_df = df_result['converted_valence'].apply(lambda x: compute_mssd_sliding_window(x, sliding_window_size))

    # Split the lists of tuples into two lists (one for each new column)
    df_result['emotioninstability_raw'], df_result['emotioninstability'] = zip(
        *results_df.apply(lambda x: zip(*x) if x else ([], [])))

    # Convert the tuples to lists
    df_result['emotioninstability_raw'] = df_result['emotioninstability_raw'].apply(list)
    df_result['emotioninstability'] = df_result['emotioninstability'].apply(list)
    df_result_copy = df_result
    df_result = pd.merge(df_result, filtered_patient_df, on='subject_reference', how='inner')
    if df_result.shape[0] == 0:
        no_rows = 1
        df_result = pd.merge(df_result_copy, all_patient_data)
    min_age = min(df_result['age'])
    max_age = max(df_result['age'])
    min_bmi = min(df_result['BMI'])
    max_bmi = max(df_result['BMI'])
    return df_result, date_min, date_max, min_age, max_age, min_bmi, max_bmi, no_rows


def emotion_inertia(df, filtered_patient_df, all_patient_data, min_sufficiency, sliding_window_size, start_date,
                    end_date):
    no_rows = 0
    df = df[['subject', 'effectiveDateTime', 'code', 'component']]

    df = pd.concat([df.drop(['subject'], axis=1), df['subject'].apply(pd.Series)], axis=1)
    ref = df['reference']
    df.drop(labels=['reference'], axis=1, inplace=True)
    df.insert(0, 'subject_reference', ref)
    df['effectiveDateTime'] = df['effectiveDateTime'].apply(lambda x: x[0:x.find('.')])
    df['effectiveDateTime'] = pd.to_datetime(df['effectiveDateTime'])
    df['effectiveDateTime'] = df['effectiveDateTime'].apply(lambda x: x.replace(microsecond=0, second=0))
    ref_date = df['effectiveDateTime'].dt.date
    ref_time = df['effectiveDateTime'].dt.time
    df.insert(3, "Date", ref_date)
    df.insert(4, "Time", ref_time)
    df_test = df['code'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    df.drop(labels=['code', 'text'], axis=1, inplace=True)
    df_test = df['coding'].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['coding'], axis=1, inplace=True)
    df_test = df[0].apply(pd.Series)
    df = pd.concat([df, df_test], axis=1)
    # drop the not required columns
    df.drop(labels=['system', 0], axis=1, inplace=True)
    df.rename(columns={'code': 'activity_code'}, inplace=True)
    date_min = min(df['Date'])
    date_max = max(df['Date'])
    df = extract_df_date_range(df, start_date, end_date, date_min, date_max)
    df = extract_emotion_values(df)
    df = df.dropna(subset=['angle', 'intensity', 'valence'])

    df['converted_valence'] = df['valence'].apply(lambda x: convert(x, -5, 5, -1, 1))
    avg_val = df.groupby(['subject_reference', 'effectiveDateTime'])[
        ['angle', 'intensity', 'valence', 'converted_valence']].mean().reset_index()
    df_result = avg_val.groupby('subject_reference')[['angle', 'intensity', 'valence', 'converted_valence']].agg(
        list).reset_index()
    df_result = df_result[
        (df_result['valence'].apply(len) >= min_sufficiency) & (df_result['angle'].apply(len) >= min_sufficiency) & (
                df_result['intensity'].apply(len) >= min_sufficiency)].reset_index(drop=True)
    df_result['angle'] = df_result['angle'].apply(lambda x: np.round(x, 2))
    df_result['intensity'] = df_result['intensity'].apply(lambda x: np.round(x, 2))
    df_result['valence'] = df_result['valence'].apply(lambda x: np.round(x, 2))
    df_result['valence'] = df_result['valence'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)

    df_result['converted_valence'] = df_result['converted_valence'].apply(lambda x: np.round(x, 2))
    df_result['converted_valence'] = df_result['converted_valence'].apply(
        lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    results_df = df_result['converted_valence'].apply(
        lambda x: compute_autocorrelation_sliding_window(x, sliding_window_size))

    # Split the lists of tuples into two lists (one for each new column)
    df_result['emotioninertia_raw'], df_result['emotioninertia'] = zip(
        *results_df.apply(lambda x: zip(*x) if x else ([], [])))

    # Convert the tuples to lists
    df_result['emotioninertia_raw'] = df_result['emotioninertia_raw'].apply(list)
    df_result['emotioninertia'] = df_result['emotioninertia'].apply(list)

    df_result_copy = df_result
    df_result = pd.merge(df_result, filtered_patient_df, on='subject_reference', how='inner')
    if df_result.shape[0] == 0:
        no_rows = 1
        df_result = pd.merge(df_result_copy, all_patient_data)
    min_age = min(df_result['age'])
    max_age = max(df_result['age'])
    min_bmi = min(df_result['BMI'])
    max_bmi = max(df_result['BMI'])
    return df_result, date_min, date_max, min_age, max_age, min_bmi, max_bmi, no_rows


def process(df, activity_code, patient_dataframe, patient_original, start_date, end_date, sliding_window, day_condition,
            min_sufficiency,
            max_sufficiency, ehi_value):
    if ehi_value == "cadphr-vo2maxra" or ehi_value == "cadphr-cadrisk10" or ehi_value == "cadphr-ecrfra":
        df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = extract_ehi_computedValue(df, ehi_value,
                                                                                                        patient_dataframe,
                                                                                                        patient_original,
                                                                                                        start_date,
                                                                                                        end_date,
                                                                                                        day_condition,
                                                                                                        sliding_window)
        df = add_grade_column(df, ehi_value)
        return df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows
    elif ehi_value == "cadphr-diabetesriskscore" or ehi_value == "cadphr-osariskscore" or ehi_value == "cadphr-hrrra":
        df_grade, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = extract_ehi_grade(df,
                                                                                                      patient_dataframe,
                                                                                                      patient_original,
                                                                                                      ehi_value,
                                                                                                      start_date,
                                                                                                      end_date)
        latest_grade = ehi_value + "_grade"
        df_grade = df_grade.rename(columns={'ehi_grade': latest_grade})
        return df_grade, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows
    elif ehi_value == "cadphr-pa" or ehi_value == "cadphr-na":
        df_roll, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = extract_ehi_columns(df, ehi_value,
                                                                                                       patient_dataframe,
                                                                                                       patient_original,
                                                                                                       activity_code,
                                                                                                       start_date,
                                                                                                       end_date,
                                                                                                       day_condition,
                                                                                                       sliding_window)
        if ehi_value == "cadphr-pa":
            df_patient = df_roll.rename(columns={'Latest': 'PA'})
            df_patient = pa_grading(df_patient)
        else:
            df_patient = df_roll.rename(columns={'Latest': 'NA'})
            df_patient = na_grading(df_patient)
        return df_patient, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows
    elif ehi_value == "cadphr-sbp" or ehi_value == "cadphr-dbp":
        df_roll, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = extract_sbp_dbp_columns(df,
                                                                                                           ehi_value,
                                                                                                           patient_dataframe,
                                                                                                           patient_original,
                                                                                                           start_date,
                                                                                                           end_date,
                                                                                                           day_condition,
                                                                                                           sliding_window)
        if ehi_value == "cadphr-sbp":
            df_patient = df_roll.rename(columns={'obs': 'SBP'})
            add_grade_column(df_patient, ehi_value)
        else:
            df_patient = df_roll.rename(columns={'obs': 'DBP'})
            add_grade_column(df_patient, ehi_value)
        return df_patient, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows
    elif ehi_value == "cadphr-pulsepressure":
        df = add_grade_column(df, ehi_value)
        return df
    elif ehi_value == "cadphr-emotioninstability" or ehi_value == "cadphr-emotioninertia" or ehi_value == "cadphr-emotionvariability":
        if ehi_value == "cadphr-emotioninertia":
            df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = emotion_inertia(df,
                                                                                                  patient_dataframe,
                                                                                                  patient_original,
                                                                                                  min_sufficiency,
                                                                                                  sliding_window,
                                                                                                  start_date,
                                                                                                  end_date)
        elif ehi_value == "cadphr-emotionvariability":
            df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = emotion_variability(df,
                                                                                                      patient_dataframe,
                                                                                                      patient_original,
                                                                                                      min_sufficiency,
                                                                                                      sliding_window,
                                                                                                      start_date,
                                                                                                      end_date)
        elif ehi_value == "cadphr-emotioninstability":
            df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = emotion_instability(df,
                                                                                                      patient_dataframe,
                                                                                                      patient_original,
                                                                                                      min_sufficiency,
                                                                                                      sliding_window,
                                                                                                      start_date,
                                                                                                      end_date)
        num_rows = len(df)
        num_first_segment = int(num_rows * 0.3333)
        num_second_segment = int(num_rows * 0.3333)
        num_third_segment = num_rows - num_first_segment - num_second_segment
        first_segment_values = np.random.randint(0, 10, num_first_segment)
        second_segment_values = np.random.randint(10, 15, num_second_segment)
        third_segment_values = np.random.randint(15, 22, num_third_segment)
        all_values = np.concatenate((first_segment_values, second_segment_values, third_segment_values))
        np.random.shuffle(all_values)
        df['GAD7'] = all_values
        df['GAD7'] = df['GAD7'].astype(int)
        if ehi_value == "cadphr-emotioninertia":
            df['emotioninertia_grades'] = df.apply(emotioninertia_grading, axis=1)
            df['inertia_latest_grade'] = df['emotioninertia_grades'].apply(get_latest_grade)
            df.rename(columns={'emotioninertia': 'Rolling AVG'}, inplace=True)
        elif ehi_value == "cadphr-emotionvariability":
            df['emotionvariability_grades'] = df['variability'].apply(emotionvariability_grading)
            df['variability_latest_grade'] = df['emotionvariability_grades'].apply(get_latest_grade)
            df.rename(columns={'variability': 'Rolling AVG'}, inplace=True)
        elif ehi_value == "cadphr-emotioninstability":
            df['emotioninstability_grades'] = df['emotioninstability'].apply(emotioninstability_grading)
            df['instability_latest_grade'] = df['emotioninstability_grades'].apply(get_latest_grade)
            df.rename(columns={'emotioninstability': 'Rolling AVG'}, inplace=True)
        return df, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows

    else:
        if ehi_value == 'cadphr-heartrate_henergy' or ehi_value == 'cadphr-hrv_henergy':
            ehi_value = 'cadphr-henergy'
        df_roll, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = extract_ehi_columns(df, ehi_value,
                                                                                                       patient_dataframe,
                                                                                                       patient_original,
                                                                                                       activity_code,
                                                                                                       start_date,
                                                                                                       end_date,
                                                                                                       day_condition,
                                                                                                       sliding_window)
        df_grade = add_grade_column(df_roll, ehi_value)
        return df_grade, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows
