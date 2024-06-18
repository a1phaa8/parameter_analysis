from flask import Flask, render_template, request, session
from data_handling.ehi_processing_functions import get_name
import datetime
import All_functions as af

app = Flask(__name__)
app.secret_key = 'my_secret_key'


@app.route('/')
def index():
    return render_template('index.html', session_message=session.get('message'))


@app.route('/result', methods=['POST'])
def result():
    # ehi_list = request.form.getlist("ehi_selection[]")
    # if len(ehi_list) == 0:
    #     return render_template('index.html', error="Select at least 1 EHI.")
    ehi_list = ["cadphr-heartrate",
    "cadphr-sbp",
    "cadphr-dbp",
    "cadphr-prq",
    "cadphr-hrv",
    "cadphr-emotioninstability",
    "cadphr-emotioninertia",
    "cadphr-emotionvariability",
    "cadphr-pa",
    "cadphr-na",
    "cadphr-affect",
    "cadphr-cadrisk10",
    "cadphr-diabetesriskscore",
    "cadphr-osariskscore",
    "cadphr-pulsepressure",
    "cadphr-henergy",
    "cadphr-vo2maxra",
    "cadphr-dprp",
    "cadphr-hrrra"]
    start_date = request.form["start_date"]
    end_date = request.form["end_date"]
    start_age = request.form["age_range_min"]
    end_age = request.form["age_range_max"]
    start_bmi = request.form["bmi_range_min"]
    end_bmi = request.form["bmi_range_max"]
    data_s = request.form["pop_type"]
    quantile = request.form.getlist("quantile")
    attribute = request.form.getlist("attribute")
    formal_ehi_name = []
    for parameter in ehi_list:
        formal_ehi_name.append(get_name(parameter))
    # formal_ehi_name = get_name_list(ehi_list)

    if start_date:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()

    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    if start_age:
        start_age = int(start_age)
    # else:
    #     start_age = 30

    if end_age:
        end_age = int(end_age)
    # else:
    #     end_age = 40

    if start_bmi:
        start_bmi = float(start_bmi)
    # else:
    #     start_bmi = 25

    if end_bmi:
        end_bmi = float(end_bmi)

    if start_date and end_date:
        if start_date > end_date:
            return render_template('index.html', error_date="Start date cannot be greater than End date")
    if start_age and end_age:
        if start_age > end_age:
            return render_template('index.html', error_age="Start age cannot be greater than End age")
    if start_bmi and end_bmi:
        if start_bmi > end_bmi:
            return render_template('index.html', error_bmi="Start BMI cannot be greater than End BMI")

    if data_s == 'none':
        data_s = "all"

    description = request.form.getlist("option_for_analysis")

    if len(quantile) == 0:
        quantile = [25, 50, 75]
    if len(attribute) == 0:
        attribute = ['grade', 'value']
    if len(description) == 0:
        description = ['visual_stats', 'descriptive_stats']
    quantile = [int(x) for x in quantile]

    # printing values requested from html page to debug and cross-check
    print("Rule List:", formal_ehi_name)
    print("Start Age:", start_age)
    print("End Age:", end_age)
    print("Start Date:", start_date)
    print("End Date:", end_date)
    print("Start BMI:", start_bmi)
    print("End BMI:", end_bmi)
    print("Gender:", data_s)
    print("Result Representation:", description)
    print("Attribute:", attribute)
    print("Quantiles:", quantile)

    # calling process_data from all_functions to process the data and pass values
    # storing values returned in variables, values like
    # date range, age range, quantiles, statistics to be displayed visual or descriptive
    # and storing list of file_paths for pie chart and histogram
    final_list, min_date, max_date, min_age, max_age, min_bmi, max_bmi, no_rows = af.print_res(ehi_list, start_age, end_age,
                                                                                      start_date, end_date,
                                                                                      start_bmi, end_bmi, quantile,
                                                                                      attribute, data_s, description)

    # converting values to required data types
    # Convert start_age, end_age to integers
    warn_age = ""
    if start_age == "" and end_age == "":
        start_age = min_age
        end_age = max_age
    elif start_age == "":
        if end_age < min_age:
            end_age = max_age
            warn_age = "(End Age out of bound. Default end age is considered)"
        start_age = min_age
    elif end_age == "":
        if start_age > max_age:
            start_age = min_age
            warn_age = "(Start Age out of bound. Default start age is considered)"
        end_age = max_age
    else:
        if ((start_age > max_age and end_age > max_age) or
            (start_age < min_age and end_age < min_age) or
            (start_age > max_age and end_age < min_age)) or start_age > max_age or end_age < min_age:
            start_age = min_age
            end_age = max_age
            warn_age = "(Age range out of bound. Default  age range is considered)"
    # Assign start_date end_date default values
    warn = None
    if start_date == "" and end_date == "":
        start_date = min_date
        end_date = max_date
    elif start_date == "":
        if end_date > max_date or end_date < min_date:
            end_date = max_date
            warn = "(End date is out of range. Default end date considered)"
        start_date = min_date
    elif end_date == "":
        if start_date < min_date or start_date > max_date:
            start_date = min_date
            warn = "(Start date is out of range. Default start date considered)"
        end_date = max_date
    else:
        if ((start_date > max_date and end_date > max_date) or (start_date < min_date and end_date < min_date) or
                (start_date > max_date and end_date < min_date)):
            start_date = min_date
            end_date = max_date
            warn = "(Input date is out of actual range, default range is considered)"

    warn_bmi = ""
    min_bmi = round(min_bmi, 2)
    max_bmi = round(max_bmi, 2)
    if start_bmi == "" and end_bmi == "":
        start_bmi = min_bmi
        end_bmi = max_bmi
    elif start_bmi == "":
        if end_bmi < min_bmi:
            end_bmi = max_bmi
            warn_bmi = "(End BMI out of bound. Default end BMI is considered)"
        start_bmi = min_bmi
    elif end_bmi == "":
        if start_bmi > max_bmi:
            start_bmi = min_bmi
            warn_bmi = "(Start BMI out of bound. Default start BMI is considered)"
        end_bmi = max_bmi
    else:
        if ((start_bmi > max_bmi and end_bmi > max_bmi) or
            (start_bmi < min_bmi and end_bmi < min_bmi) or
            (start_bmi > max_bmi and end_bmi < min_bmi)) or start_bmi > max_bmi or end_bmi < min_bmi:
            start_bmi = min_bmi
            end_bmi = max_bmi
            warn_bmi = "(BMI range out of bound. Default BMI range is considered)"

    if len(quantile) == 0:
        quantile = "No input given"

    empty_warn = ""
    if no_rows:
        start_age = min_age
        end_age = max_age
        start_bmi = min_bmi
        end_bmi = max_bmi
        start_date = min_date
        end_date = max_date
        data_s = "all"
        empty_warn = "All cohort considered as filtered cohort has no data"

    date_range = f"{start_date} to {end_date}"
    age_range = f"{start_age} to {end_age}"
    bmi_range = f"{start_bmi} to {end_bmi}"
    return render_template('result.html',
                           ehi=formal_ehi_name,
                           age_range=age_range,
                           date_range=date_range,
                           quantile=quantile,
                           stats=attribute,
                           final_lst=final_list,
                           data_type=data_s,
                           des_opt=description,
                           attribute_type=attribute,
                           warning_message=warn,
                           BMI_range=bmi_range,
                           age_warn=warn_age,
                           bmi_warn=warn_bmi,
                           cohort_warn=empty_warn
                           )


if __name__ == '__main__':
    app.run(debug=True)
