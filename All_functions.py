from data_handling.descriptive_stats import *
from data_handling.return_required_dataframes import *
from data_handling.visual_stats import *
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# start_date = data["start_Date"]
# end_date = data["end_Date"]
# data_select = data["gender"]
# start_age, end_age = data["start_age"], data["end_age"]
# BMI_lower_limit, BMI_upper_limit = data["BMI_start"], data["BMI_end"]
# stats = data["attribute"]
# des_opt = data["display_options"]
# ehi = data["EHI"]


def compare_dates(strt_d, end_d, min_d, max_d):
    if strt_d != "" and end_d != "":
        if (strt_d < min_d and end_d > max_d) or (strt_d < min_d and end_d < min_d) or (
                strt_d > max_d and end_d > max_d) or (strt_d > max_d and end_d < min_d):
            strt_d = min_d
            end_d = max_d
        elif end_d > max_d:
            end_d = max_d
        elif strt_d < min_d:
            strt_d = min_d
    elif strt_d != "":
        if strt_d < min_d or strt_d > max_d:
            strt_d = min_d
        end_d = max_d
    elif end_d != "":
        if end_d > max_d or end_d < min_d:
            end_d = max_d
        strt_d = min_d
    return strt_d, end_d


def compare_age(start_age, end_age, min_age, max_age):
    if start_age and end_age:
        if start_age > max_age or start_age < min_age:
            print("WARNING\n")
            start_age = min_age
        if end_age < min_age or end_age > max_age:
            end_age = max_age
    if start_age == "" and end_age == "":
        start_age = min_age
        end_age = max_age
    elif start_age == "":
        if end_age > max_age or end_age < min_age:
            end_age = max_age
        start_age = min_age
    elif end_age == "":
        if start_age < min_age or start_age > max_age:
            start_age = min_age
        end_age = max_age
    else:
        if ((start_age < min_age and end_age > max_age) or (start_age > max_age and end_age > max_age) or (
                start_age < min_age and end_age < min_age) or
                (start_age > max_age and end_age < min_age)):
            start_age = min_age
            end_age = max_age
        elif start_age < min_age:
            start_age = min_age
        elif end_age > max_age:
            end_age = max_age
    return start_age, end_age


def get_final_result(ehi, start_age, end_age, start_date, end_date, start_bmi, end_bmi, quantile, stats, data_sel, des_opt):
    output_lst = []
    final_dataframe, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = process_data(ehi, start_age, end_age,
                                                                                           start_date, end_date,
                                                                                           start_bmi, end_bmi, data_sel)
    start_date, end_date = compare_dates(start_date, end_date, min_date, max_date)
    start_age, end_age = compare_age(start_age, end_age, min_age, max_age)
    # displaying dataframes of desired ehi on console
    for ehi_parameter, dataframe in final_dataframe.items():
        print(f"Processed DataFrame for EHI {ehi_parameter}:")
        print(dataframe)
    if no_rows:
        start_age = min_age
        end_age = max_age
        start_date = min_date
        end_date = max_date

    if ('grade' in stats) & ('value' in stats):
        print("both selected")
        if ('descriptive_stats' in des_opt) & ('visual_stats' in des_opt):
            for ehi_parameter, dataframe in final_dataframe.items():
                ehi_name = get_name(ehi_parameter)
                descriptive_stats_grade = grade_description(dataframe, ehi_parameter, quantile)
                descriptive_stats_avg = rolling_average_description(dataframe, ehi_parameter, quantile)
                file1 = histogram_grade(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                file2 = pie_chart_grade(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                file3 = histogram_roll_avg(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                combined_lst = [[file1, file2], [descriptive_stats_grade, descriptive_stats_avg], [ehi_name], [file3]]
                output_lst.append(combined_lst)
            return output_lst, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows

        elif 'descriptive_stats' in des_opt:
            for ehi_parameter, dataframe in final_dataframe.items():
                ehi_name = get_name(ehi_parameter)
                descriptive_stats_grade = grade_description(dataframe, ehi_parameter, quantile)
                descriptive_stats_avg = rolling_average_description(dataframe, ehi_parameter, quantile)
                combined_des_lst = [descriptive_stats_grade, descriptive_stats_avg, ehi_name]
                output_lst.append(combined_des_lst)
            return output_lst, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows

        elif 'visual_stats' in des_opt:
            for ehi_parameter, dataframe in final_dataframe.items():
                ehi_name = get_name(ehi_parameter)
                file1 = histogram_grade(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                file2 = pie_chart_grade(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                file3 = histogram_roll_avg(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                combined_figure_lst = [[file1, file2], [ehi_name], [file3]]
                output_lst.append(combined_figure_lst)
            return output_lst, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows

    elif 'grade' in stats:
        print("grade is selected")
        if ('descriptive_stats' in des_opt) & ('visual_stats' in des_opt):
            output_lst = []
            for ehi_parameter, dataframe in final_dataframe.items():
                ehi_name = get_name(ehi_parameter)
                descriptive_stats_grade = grade_description(dataframe, ehi_parameter, quantile)
                file1 = histogram_grade(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                file2 = pie_chart_grade(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                combined_lst = [[file1, file2], [descriptive_stats_grade], [ehi_name]]
                output_lst.append(combined_lst)
            return output_lst, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows

        elif 'descriptive_stats' in des_opt:
            output_lst = []
            for ehi_parameter, dataframe in final_dataframe.items():
                ehi_name = get_name(ehi_parameter)
                descriptive_stats_grade = grade_description(dataframe, ehi_parameter, quantile)
                combined_des_lst = [descriptive_stats_grade, ehi_name]
                output_lst.append(combined_des_lst)
            return output_lst, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows

        elif 'visual_stats' in des_opt:
            for ehi_parameter, dataframe in final_dataframe.items():
                ehi_name = get_name(ehi_parameter)
                file1 = histogram_grade(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                file2 = pie_chart_grade(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                combined_figure_lst = [[file1, file2], [ehi_name]]
                output_lst.append(combined_figure_lst)
            return output_lst, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows
    elif 'value' in stats:
        print("value is selected")
        if ('descriptive_stats' in des_opt) & ('visual_stats' in des_opt):
            for ehi_parameter, dataframe in final_dataframe.items():
                ehi_name = get_name(ehi_parameter)
                descriptive_stats_avg = rolling_average_description(dataframe, ehi_parameter, quantile)
                file3 = histogram_roll_avg(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                combined_lst = [[file3], [descriptive_stats_avg], [ehi_name]]
                output_lst.append(combined_lst)
            return output_lst, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows

        elif 'descriptive_stats' in des_opt:
            for ehi_parameter, dataframe in final_dataframe.items():
                ehi_name = get_name(ehi_parameter)
                descriptive_stats_avg = rolling_average_description(dataframe, ehi_parameter, quantile)
                combined_des_lst = [descriptive_stats_avg, ehi_name]
                output_lst.append(combined_des_lst)
            return output_lst, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows

        elif 'visual_stats' in des_opt:
            for ehi_parameter, dataframe in final_dataframe.items():
                ehi_name = get_name(ehi_parameter)
                file3 = histogram_roll_avg(dataframe, ehi_parameter, start_age, end_age, start_date, end_date, data_sel)
                combined_figure_lst = [[file3], [ehi_name]]
                output_lst.append(combined_figure_lst)
            return output_lst, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows
