import os
from textwrap import wrap
import warnings
import matplotlib
import matplotlib.pyplot as plt
from data_handling import return_required_dataframes as rd

matplotlib.use('Agg')
warnings.filterwarnings("ignore")


# creating histogram for grade attribute
def histogram_grade(df, ehi, start_age, end_age, start_d, end_d, data_cond):
    name = rd.get_name(ehi)
    if ehi in ['cadphr-emotioninstability', 'cadphr-emotioninertia', 'cadphr-emotionvariability',
               'cadphr-emotionmeasure']:
        grade_str = ehi.split("-emotion")
        grade_column = grade_str[1] + "_latest_grade"
    elif ehi in ['cadphr-heartrate']:
        grade_column = f'{ehi}_grade'
    elif ehi in ['cadphr-cvreactivity']:
        grade_column = 'conclusion'
    else:
        grade_column = f'{ehi}_grade'
        df[grade_column] = df[grade_column].astype(float)
    s = df[grade_column].explode()
    if s.dtype == 'object' and ehi not in ['cadphr-cvreactivity']:
        s = s.explode().astype(float)
    plt.hist(s, bins=30, edgecolor='red')
    plt.xlabel('Grade')
    plt.ylabel('Frequency')
    title = f'Histogram for {name}(Grades)'
    if start_age != "" and end_age != "":
        title += f' with age group ({start_age}-{end_age})'
    elif start_age != "":
        title += f' with age group >= {start_age}'
    elif end_age != "":
        title += f' with age group <= {end_age}'
    else:
        title += ' for all age groups'

    if data_cond in ['all', 'male', 'female']:
        title += f' for gender({data_cond})'

    if start_d != "" and end_d != "":
        title += f' during {start_d} to {end_d}'
    elif start_d != "":
        title += f' from {start_d} onwards'
    elif end_d != "":
        title += f' up to {end_d}'
    else:
        title += ' for all dates'

    plt.title("\n".join(wrap(title)))
    plt.tight_layout()
    filename = "histogram_fig_" + grade_column + ".png"
    static_folder = os.path.join(os.getcwd(), 'static/figures')  # Assumes 'static' is in the current working directory
    file_path = os.path.join(static_folder, filename)
    plt.savefig(file_path)
    plt.close()
    return filename


# creating histogram for rolling average attribute
def histogram_roll_avg(df, ehi, start_age, end_age, start_d, end_d, data_cond):
    name = rd.get_name(ehi)
    if ehi in ['cadphr-osariskscore', 'cadphr-diabetesriskscore', 'cadphr-cvreactivity', 'cadphr-hrrra']:
        print("Rolling AVG Not Available for these parameters")
        filename = ""
        return filename
    elif ehi in ['cadphr-henergy']:
        s = df["InstaBEATS"].explode()
    elif ehi in ['cadphr-pulsepressure']:
        s = df["Pulse Pressure"].explode()
    elif ehi in ['cadphr-affect']:
        s = df["affect"].explode()
    else:
        s = df["Rolling AVG"].explode()
    if s.dtype == 'object':
        s = s.explode().astype(float)
    plt.hist(s, bins=30, edgecolor='red')
    plt.xlabel('Average Values')
    plt.ylabel('Frequency')
    title = f'Histogram for {name}(Values)'
    if start_age != "" and end_age != "":
        title += f' with age group ({start_age}-{end_age})'
    elif start_age != "":
        title += f' with age group >= {start_age}'
    elif end_age != "":
        title += f' with age group <= {end_age}'
    else:
        title += ' for all age groups'

    if data_cond in ['all', 'male', 'female']:
        title += f' for gender({data_cond})'

    if start_d != "" and end_d != "":
        title += f' during {start_d} to {end_d}'
    elif start_d != "":
        title += f' from {start_d} onwards'
    elif end_d != "":
        title += f' up to {end_d}'
    else:
        title += ' for all dates'
    plt.title("\n".join(wrap(title)))
    plt.tight_layout()
    filename = "histogram_roll_avg_" + name + ".png"
    static_folder = os.path.join(os.getcwd(), 'static/figures')  # Assumes 'static' is in the current working directory
    file_path = os.path.join(static_folder, filename)
    plt.savefig(file_path)
    plt.close()
    return filename


def pie_chart_grade(df, ehi, start_age, end_age, start_d, end_d, data_cond):
    name = rd.get_name(ehi)
    if ehi in ['cadphr-emotioninstability', 'cadphr-emotioninertia', 'cadphr-emotionvariability',
               'cadphr-emotionmeasure']:
        grade_str = ehi.split("-emotion")
        grade_column = grade_str[1] + "_latest_grade"
    elif ehi in ['cadphr-heartrate']:
        grade_column = f'{ehi}_grade'
    elif ehi in ['cadphr-cvreactivity']:
        grade_column = 'conclusion'
    else:
        grade_column = f'{ehi}_grade'
        df[grade_column] = df[grade_column].astype(int)
    s = df[grade_column].explode()
    if s.dtype == 'object' and ehi not in ['cadphr-cvreactivity']:
        s = s.explode().astype(float)
    grade_counts = s.value_counts().sort_index(ascending=False)
    total_values = len(s)
    percentages = grade_counts / total_values * 100

    # Check if any percentage is less than 5%
    for percentage in percentages:
        if percentage < 5:
            explode = [0.15] * len(grade_counts)
        else:
            explode = [0.08] * len(grade_counts)

    # Define custom colors
    value_color_mapping_5_grades = {
        1: 'orangered',
        2: 'orange',
        3: 'lightgreen',
        4: 'olive',
        5: 'darkgreen'
    }

    value_color_mapping_3_grades = {
        1: 'orangered',
        2: 'lightgreen',
        3: 'green'
    }
    value_color_mapping_4_grades = {
        1: 'orangered',
        2: 'orange',
        3: 'lightgreen',
        4: 'darkgreen'
    }

    value_color_mapping_6_grades = {
        1: 'orangered',
        2: 'orange',
        3: 'lightcoral',
        4: 'lightgreen',
        5: 'olive',
        6: 'darkgreen'
    }
    value_color_mapping_2_grades = {
        1: 'orange',
        2: 'green'
    }

    value_color_mapping_cv_grades = {
        'Normal': 'green',
        'Exaggerated': 'orange'
    }

    if ehi in ['cadphr-emotioninstability', 'cadphr-emotioninertia', 'cadphr-emotionvariability',
               'cadphr-diabetesriskscore', 'cadphr-hrv']:
        colors = [value_color_mapping_3_grades.get(value, 'gray') for value in grade_counts.index]
    elif ehi in ['cadphr-dprp']:
        colors = [value_color_mapping_6_grades.get(value, 'gray') for value in grade_counts.index]
    elif ehi in ['cadphr-osariskscore', 'cadphr-hrrra']:
        colors = [value_color_mapping_2_grades.get(value, 'gray') for value in grade_counts.index]
    elif ehi in ['cadphr-cvreactivity']:
        colors = [value_color_mapping_cv_grades.get(value, 'gray') for value in grade_counts.index]
    elif ehi in ['cadphr-affect']:
        colors = [value_color_mapping_4_grades.get(value, 'gray') for value in grade_counts.index]
    else:
        colors = [value_color_mapping_5_grades.get(value, 'gray') for value in grade_counts.index]
    plt.pie(grade_counts, labels=grade_counts.index, autopct='%1.1f%%', startangle=150, explode=explode, colors=colors)
    title = f'Pie Chart for {name}(Grades)'
    if start_age != "" and end_age != "":
        title += f' with age group ({start_age}-{end_age})'
    elif start_age != "":
        title += f' with age group >= {start_age}'
    elif end_age != "":
        title += f' with age group <= {end_age}'
    else:
        title += ' for all age groups'

    if data_cond in ['all', 'male', 'female']:
        title += f' for gender({data_cond})'

    if start_d != "" and end_d != "":
        title += f' during {start_d} to {end_d}'
    elif start_d != "":
        title += f' from {start_d} onwards'
    elif end_d != "":
        title += f' up to {end_d}'
    else:
        title += ' for all dates'
    plt.title("\n".join(wrap(title)))
    plt.tight_layout()
    filename = "pie_chart_fig_" + grade_column + ".png"
    static_folder = os.path.join(os.getcwd(), 'static/figures')
    file_path = os.path.join(static_folder, filename)
    plt.savefig(file_path)
    plt.close()
    return filename
