import numpy as np
import statistics
import warnings
from data_handling import return_required_dataframes as rd

warnings.filterwarnings("ignore")


def get_percentiles(s, quantiles):
    percentiles = {}
    for q in quantiles:
        percentiles[f"Percentile {q}"] = round(np.percentile(s, q), 2)

    return percentiles


def grade_description(df, ehi, quantiles):
    name = rd.get_name(ehi)
    # extracting grade column name
    if ehi in ['cadphr-emotioninstability', 'cadphr-emotioninertia', 'cadphr-emotionvariability',
               'cadphr-emotionmeasure']:
        grade_str = ehi.split("-emotion")
        grade_column = grade_str[1] + "_latest_grade"
    elif ehi in ['cadphr-heartrate']:
        grade_column = f'{ehi}_grade'
        df[grade_column] = df[grade_column].astype(float)
    elif ehi in ['cadphr-cvreactivity']:
        grade_column = 'conclusion'
        s = df[grade_column].explode()
        list_val = s.dropna().tolist()
        mode_value = s.mode().iloc[0]
        descriptive_stats = s.describe()
        count_normal = 0
        count_exaggerated = 0
        for x in list_val:
            if x == 'Normal':
                count_normal += 1
            else:
                count_exaggerated += 1
        percent_normal = count_normal / descriptive_stats['count'] * 100
        percent_exaggerated = count_exaggerated / descriptive_stats['count'] * 100
        descriptive_stats_str = (f"<br><b>{name} Grade Statistics:</b><br>"
                                 "The dataset contains {:.0f} unique users.<br>"
                                 "The count of Exaggerated grade is {:.2f}<br>"
                                 "The count of Normal grade is {:.2f}<br>"
                                 "The percentage of Exaggerated grade is {:.2f}<br>"
                                 "The percentage of Normal grade is {:.2f}<br>"
                                 "Mode in Grade list is: {}<br>").format(
            descriptive_stats['count'],
            count_exaggerated,
            count_normal,
            percent_exaggerated,
            percent_normal,
            mode_value
        )
        return descriptive_stats_str

    else:
        grade_column = f'{ehi}_grade'
        df[grade_column] = df[grade_column].astype(float)
    unique_users = df['subject_reference'].nunique()
    s = df[grade_column].explode()
    list_val = s.dropna().tolist()
    quantile_result = get_percentiles(list_val, quantiles)
    mode_value = s.mode().iloc[0]
    descriptive_stats = s.describe()
    # noinspection PyStringFormat
    descriptive_stats_str = f"<br><b>{name} Grade Statistics:</b><br>" \
                            "The dataset contains {:.0f} unique users.<br>" \
                            "The average (mean) grade is {:.2f}, with a standard deviation of {:.2f}.<br>" \
                            "The grades range from a minimum of {:.2f} to a maximum of {:.2f}.<br>" \
                            "Percentile wanted: {}<br>" \
                            "Mode in Grade list is: {:.2f}<br>".format(
        unique_users,
        descriptive_stats['mean'],
        descriptive_stats['std'],
        descriptive_stats['min'],
        descriptive_stats['max'],
        quantile_result,
        round(mode_value, 2)
    )

    return descriptive_stats_str


def rolling_average_description(df, ehi, quantiles):
    name = rd.get_name(ehi)
    if ehi in ['cadphr-osariskscore', 'cadphr-diabetesriskscore', 'cadphr-cvreactivity', 'cadphr-hrrra']:
        print("Rolling AVG Not Available for these parameters")
        filename = ""
        return filename
    elif ehi in ['cadphr-henergy']:
        # df["InstaBEATS"] = df["InstaBEATS"].astype(float)
        data = [val for sublist in df['InstaBEATS'] for val in sublist]
    elif ehi in ['cadphr-pulsepressure']:
        # df["Pulse Pressure"] = df["Pulse Pressure"].astype(float)
        data = [val for sublist in df['Pulse Pressure'] for val in sublist]
    elif ehi in ['cadphr-affect']:
        data = [val for val in df['affect']]
    else:
        data = [val for sublist in df['Rolling AVG'] for val in sublist]

    for value in data.copy():
        if np.isnan(value):
            data.remove(value)

    unique_users = df['subject_reference'].nunique()
    quantile_result = get_percentiles(data, quantiles)
    if len(data) >= 2:
        std_dev = f", with a standard deviation of {statistics.stdev(data):.2f}"
    else:
        std_dev = " *cannot calculate standard deviation with one value*"

    result_messages = f"<br><b>Rolling Average Statistics for {name}:</b><br>" \
                      f"Total number of unique users: {unique_users}<br>" \
                      f"The average (mean) rolling average of the observations is {statistics.mean(data):.2f}{std_dev}.<br>" \
                      f"The rolling average range from a minimum of {min(data):.2f} to a maximum of {max(data):.2f}.<br>" \
                      f"Percentile wanted: {quantile_result}<br>" \
                      f"Mode in Rolling AVG list is: {statistics.mode(data):.2f}<br>"

    return result_messages
