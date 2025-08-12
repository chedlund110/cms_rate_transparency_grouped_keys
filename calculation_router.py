from calculations.fee_schedule import process_fee_schedule
from calculations.grouper import (
    calc_asc_grouper_9lv_no_disc,
    calc_asc_grouper_base
)
from calculations.case_rate import(
    process_case_rate,
    process_case_rate_limit,
    process_case_rate_two_lev_per_diem_limit,
    process_case_rate_three_lev_per_diem_limit,
    process_cr_ltd_by_pct_of_chg
)
from calculations.drg import (
    process_drg_weighting,
    process_drg_weighting_day_outlier
)
from calculations.limits import (
    process_limit,
    process_limit_allowed,
    process_limit_allowed_percent
)
from calculations.misc_calcs import (
    process_flat_dollar_discount,
    process_ndc,
    process_per_item,
    process_percent_plus_excess,
    process_unit_ltd_by_chg,
    process_visit_plus_rate_per_hour,
    process_optum_physician_pricer
)
from calculations.percent_allowed import (
    process_percent_of_allowed,
    process_percent_of_allowed_plus_fd_amt
)
from calculations.percent_charges import (
    process_pct_chg_pd_max,
    process_pct_chg_pd_max_01,
    process_pct_chg_per_proc_max,
    process_pct_chg_per_unit_threshold,
    process_percent_threshold,
    process_percent_of_charges, 
    process_pct_of_chrg_flat_amt,
    process_percent_of_charges_max,
    process_percent_of_charges_max_01
)
from calculations.per_diem import (
    process_per_diem,
    process_pd_with_max,
    process_three_lev_pd,
    process_pd_five_lv_confine_day,
    process_pd_with_alos
)
# different calculations may use the same function

CALCULATION_ROUTER = {
    'CalcASCGrouper9LvNoDisc': calc_asc_grouper_9lv_no_disc,
    'CalcASCGrouperBase': calc_asc_grouper_base,
    'CalcCaseRate': process_case_rate,
    'CalcCaseRateLimit': process_case_rate_limit,
    'CalcCaseRateTwoLevPerDiemLimit': process_case_rate_two_lev_per_diem_limit,
    'CalcCaseRateThreeLevPerDiemLimit': process_case_rate_three_lev_per_diem_limit,
    'CalcCRLtdByPctOfChg': process_cr_ltd_by_pct_of_chg,
    'CalcDRGWeighting': process_drg_weighting,
    'CalcDRGWeightingDayOutlier': process_drg_weighting_day_outlier,
    'CalcFlatDollarDiscount': process_flat_dollar_discount,
    'CalcLimit': process_limit,
    'CalcLimitAllowedPercent': process_limit_allowed_percent,
    'CalcNDC': process_ndc,
    'CalcNtwxStdFeeSched': process_fee_schedule,
    'CalcOptumPhysicianPricer': process_optum_physician_pricer,
    'CalcPctChgPDMax': process_pct_chg_pd_max,
    'CalcPctChgPDMax_01': process_pct_chg_pd_max_01,
    'CalcPctChgPerUnitThreshold': process_pct_chg_per_unit_threshold,
    'CalcPercentThresh': process_percent_threshold,
    'CalcPercentOfChargesMax_01': process_percent_of_charges_max_01,
    'CalcPercentOfNtwxStdFeeSched': process_fee_schedule,
    'CalcPercentOfCharges': process_percent_of_charges,
    'CalcPercentOfChargesMax': process_percent_of_charges_max,
    'CalcPctChgPerProcMax': process_pct_chg_per_proc_max,
    'CalcPctOfChrgFlatAmt': process_pct_of_chrg_flat_amt,
    'CalcPercentOfAllowed': process_percent_of_allowed,
    'CalcPercentOfAllowedPlusFDAmt': process_percent_of_allowed_plus_fd_amt,
    'CalcPerDiem': process_per_diem,
    'CalcThreeLevPD': process_three_lev_pd,
    'CalcPDwithMax': process_pd_with_max,
    'CalcPDwithALOS': process_pd_with_alos,
    'CalcPDFiveLvConfineDay': process_pd_five_lv_confine_day,
    'CalcPerItem': process_per_item,
    'CalcUnitLtdByChg': process_unit_ltd_by_chg,
    'CalcPercentPlusExcess': process_percent_plus_excess,
    'CalcVisitPlusRatePerHour': process_visit_plus_rate_per_hour
}
