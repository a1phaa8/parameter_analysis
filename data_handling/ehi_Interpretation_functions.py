def ehi_Interpretation_hrrra(row):
    if row['cadphr-hrrra_grade'] == 1 or row['cadphr-hrrra_grade'] == 2:
        return 'Amber'
    else:
        return 'Green'


def ehi_Interpretation_vo2max(row):
    if row['ehiGrading'] == 1 or row['ehiGrading'] == 2:
        return 'Amber'
    else:
        return 'Green'


def ehi_Interpretation_drs(row):
    if row['ehiGrading'] == 1 or row['ehiGrading'] == 2:
        return 'Amber'
    else:
        return 'Green'


def ehi_Interpretation_cad(row):
    if row['ehiGrading'] == 1:
        return 'Amber'
    else:
        return 'Green'


def ehi_Interpretation_osa(row):
    if row['ehiGrading'] == 1:
        return 'Amber'
    else:
        return 'Green'


def ehi_Interpretation_ecrf(row):
    if row['ehiGrading'] == 1 or row['ehiGrading'] == 2:
        return 'Amber'
    else:
        return 'Green'


def ehi_Interpretation_prq(row):
    if row['cadphr-prq_grade'] == 1 or row['cadphr-prq_grade'] == 2 or row['cadphr-prq_grade'] == 3:
        return '2-Amber'
    else:
        return '3-Green'


def ehi_Interpretation_hrv(row):
    if row['cadphr-hrv_grade'] == 1:
        return 'Amber'
    else:
        return 'Green'


def ehi_Interpretation_hr(row):
    if row['cadphr-heartrate_grade'] == '1' or row['cadphr-heartrate_grade'] == '2':
        return 'Amber'
    else:
        return 'Green'


def ehi_Interpretation_sbp(row):
    if row['cadphr-sbp_grade'] == 1:
        return 'Red'
    elif row['cadphr-sbp_grade'] == 2 or row['cadphr-sbp_grade'] == 3:
        return 'Amber'
    else:
        return 'Green'


def ehi_Interpretation_dbp(row):
    if row['cadphr-sbp_grade'] == 1:
        return 'Red'
    elif row['cadphr-sbp_grade'] == 2 or row['cadphr-sbp_grade'] == 3:
        return 'Amber'
    else:
        return 'Green'


def ehi_Interpretation_pp(row):
    if row['cadphr-pulsepressure_grade'] == 1 or row['cadphr-pulsepressure_grade'] == 2:
        return 'Amber'
    else:
        return 'Green'
