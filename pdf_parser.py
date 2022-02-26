from pdfminer.layout import LAParams, LTFigure, LTImage
from pdfminer.converter import PDFPageAggregator
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
import re
import os
from time import strptime
from typing import BinaryIO
from metrics import metrics_stop_list


def parse_date(date_text) -> tuple:
    """
    Parse date range from a text string.
    :param date_text:   unformatted date range string
    :return:            (date_range_start, date_range_end) in 'YYYY-mm-DD' format
    """
    def convert_date(date_list) -> str:
        return '-'.join([date_list[2], str(strptime(date_list[0], '%b').tm_mon).rjust(2, '0'), date_list[1]])

    temp = date_text.split()
    date_range_start = [i.replace(',', '').rjust(2, '0') for i in temp[3:6]]
    date_range_end = [i.replace(',', '').rjust(2, '0') for i in temp[9:12]]
    return convert_date(date_range_start), convert_date(date_range_end)


def parse_document(stream: BinaryIO) -> dict:
    """
    Parse text elements from pdf document.
    :param stream:  buffered stream of a pdf doc
    :return:        dict(text_element_coordinates: text)
    """
    elements = {}
    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    for page in PDFPage.get_pages(stream):
        interpreter.process_page(page)
        layout = device.get_result()
        for element in layout:
            if isinstance(element, LTFigure):
                text = ''.join([i.get_text() for i in element._objs if not isinstance(i, LTImage)])
                if 'Account activity from' in text:
                    elements['date_range'] = parse_date(text)
                elif text in metrics_stop_list:
                    continue
                key = tuple(round(i, 2) for i in element.bbox)
                elements[key] = text
    return elements


def metric_segmentation(text_elements: dict) -> dict:
    """
    Map a value to a metric by coordinates.
    :param text_elements:   dict(text_element_coordinates: text)
    :return:                dict(date_range: metrics)
    """
    date_range = text_elements.pop('date_range')

    elements = {k: v for k, v in sorted(text_elements.items(), key=lambda item: item[0])}  # sorting
    temp_dict = {}
    [temp_dict.setdefault(i, {}).update({k: v}) for k, v in elements.items()
     for i in set([k2[1] for k2 in elements.keys()]) if
     k[1] == i]  # merge elements by y0 position (union elements by line)
    # sorting
    temp_dict = {k: v for k, v in sorted(temp_dict.items(), key=lambda item: item[0])}
    # single elements with y0 deviation
    single_elements = {k: v for k, v in temp_dict.items() if len(v) == 1}

    tokens = [1 if abs(list(single_elements.keys())[i] - list(single_elements.keys())[i + 1]) <= 1.15 else 0
              for i in range(len(single_elements) - 1)]  # tokens to merge single elements
    tokens.append(0)
    token_groups = [[j for j in range(i, i + tokens[i:].index(0) + 1)] for i in range(len(tokens)) if
                    tokens[i] == 1 and tokens[i - 1] != 1]  # sequences to merge by coordinates

    #  merging single elements
    merged_elements = {}
    for subsequence in token_groups:
        tmp_elements = {}
        key = None
        for i in subsequence:
            if not key:
                key = list(single_elements.keys())[i]
            tmp_elements.update(list(single_elements.values())[i])
        merged_elements[key] = tmp_elements

    # get metric and indicator pairs
    metrics_list = [list(i.values()) for i in merged_elements.values() if len(i) == 2] + \
                   [list(i.values()) for i in temp_dict.values() if len(i) == 2]
    # clean pairs
    metrics_list = [pair for pair in metrics_list if sum([re.sub(r'[,.-]', '', el).isdigit() for el in pair]) == 1]
    metrics_dict = {item[re.sub(r'[,.-]', '', item[0]).isdigit()]:
                        float(item[re.sub(r'[,.-]', '', item[1]).isdigit()].replace(',', '')) for item in metrics_list}

    return {date_range: metrics_dict}


def main_pdf_parser(pdf_folder) -> dict:
    """
    Main function for parsing pdf files.
    :param pdf_folder:  absolute path to pdf folder
    :return:            dict(date_range: metrics)
    """
    result = {}
    for file in (i for i in os.listdir(pdf_folder) if not i.startswith('~') and i.endswith('pdf')):
        with open(os.path.join(pdf_folder, file), 'rb') as fp:
            txt_elements = parse_document(fp)
            result.update(metric_segmentation(txt_elements))
    return result


if __name__ == '__main__':
    test = main_pdf_parser(r'C:\Users\19213301\Desktop\pdf\qa\Reports')
    print()
