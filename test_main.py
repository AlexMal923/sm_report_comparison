from pdf_parser import main_pdf_parser
from api_async import main_api
import time
import pytest
import os


def reformat_report(report: dict) -> dict:
    """
    Reformat SellerMetrix report to metric value pairs
    :param report:  SellerMetrix report
    :return:        dict(metric: value)
    """
    report_metrics = {}
    content = report.get('marketplaces', [0])[0]
    if content:
        income = content.get('data', {}).get('Income', {})
        expenses = content.get('data', {}).get('Amazon Expenses', {})
        for metric, value in [(metric, value) for i in [income, expenses] for metric, value in i.items()]:
            report_metrics[metric] = list(list(value)[0].values())[0] if isinstance(list(value)[0], dict) else float()
    return report_metrics


def main(marketplace_id, pdf_folder=os.getcwd()) -> dict:
    """
    The main function for collection and mapping reports.
    :param marketplace_id:  marketplace identifier
    :param pdf_folder:      path to Amazon pdf reports
    :return:                dict(date_range: [sm_report, amazon_report]
    """
    amazon_reports = main_pdf_parser(pdf_folder)
    sm_reports = main_api(marketplace_id, list(amazon_reports.keys()))
    #
    reports = {date_range: [reformat_report(sm_reports.get(date_range, {})), amazon_reports.get(date_range, {})]
               for date_range in list(amazon_reports)}
    # reports = [[reformat_report(sm_reports.get(date_range, {})), amazon_reports.get(date_range, {})]
    #            for date_range in list(amazon_reports)]

    return reports


# @pytest.mark.parametrize('sm_report, amazon_report', test_data)
# def test_compare_metrics(sm_report, amazon_report):
#     assert sm_report == amazon_report
#
#
# test_data = main(3, r'C:\Users\79089\Desktop\qa\Reports')
# for k, v in test_data.items():
#     print(k)
#     for metric in set(v[0]).intersection(set(v[1])):
#         print(metric)
#         test_compare_metrics(v[0][metric], v[1][metric])

