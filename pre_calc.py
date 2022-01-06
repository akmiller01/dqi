import argparse
import glob
import json
from lxml import etree
from lxml.etree import XMLParser
import os
import progressbar
import requests
import csv


def destroy_tree(tree):
    root = tree.getroot()

    node_tracker = {root: [0, None]}

    for node in root.iterdescendants():
        parent = node.getparent()
        node_tracker[node] = [node_tracker[parent][0] + 1, parent]

    node_tracker = sorted([(depth, parent, child) for child, (depth, parent)
                           in node_tracker.items()], key=lambda x: x[0], reverse=True)

    for _, parent, child in node_tracker:
        if parent is None:
            break
        parent.remove(child)

    del tree


large_parser = XMLParser(huge_tree=True)
parser = etree.XMLParser(remove_blank_text=True)

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='Create publisher metadata')
    arg_parser.add_argument('publisher', type=str, help='Publisher\'s ID from the IATI Registry')
    args = arg_parser.parse_args()
    output_dir = os.path.join("output", args.publisher)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)


    indicators = [
        # (Category, Indicator, xpath numerator, xpath denominator),
        ("Data Completeness", "Mandatory & Recommended (reporting org)", "/iati-activities/iati-activity[reporting-org[string-length(@ref) > 0]]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Mandatory & Recommended (identifier)", "/iati-activities/iati-activity[string-length(iati-identifier) > 0]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Mandatory & Recommended (participating org)", "/iati-activities/iati-activity[participating-org]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Mandatory & Recommended (title)", "/iati-activities/iati-activity[string-length(title) > 0]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Mandatory & Recommended (description)", "/iati-activities/iati-activity[string-length(description) > 0]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Mandatory & Recommended (activity status)", "/iati-activities/iati-activity[activity-status]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Mandatory & Recommended (activity date)", "/iati-activities/iati-activity[activity-date[@type='1' or @type='2']]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Mandatory & Recommended (budget)", "/iati-activities/iati-activity[(string-length(@budget-not-provided) > 0) or  boolean(budget)]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Mandatory & Recommended (transaction)", "/iati-activities/iati-activity[transaction]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Location", "/iati-activities/iati-activity[(location/name[string-length(narrative) > 0]) | (location/description[string-length(narrative) > 0]) | (location/administrative[string-length(@code) > 0]) | (location/point[string-length(pos) > 0])]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Classification (sector)", "/iati-activities/iati-activity[sector[@vocabulary='1' or @vocabulary='2' or not(@vocabulary)] | transaction[sector[@vocabulary='1' or @vocabulary='2' or not(@vocabulary)]]]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Classification (aid type)", "/iati-activities/iati-activity[(string-length(default-aid-type/@code) > 0) or (string-length(transaction/aid-type/@code) > 0)]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Classification (policy marker)", "/iati-activities/iati-activity[policy-marker[@vocabulary='1' or not(@vocabulary)]]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Classification (finance type)", "/iati-activities/iati-activity[(string-length(default-finance-type/@code) > 0) or (string-length(transaction/finance-type/@code) > 0)]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Classification (flow type)", "/iati-activities/iati-activity[(string-length(default-flow-type/@code) > 0) or (string-length(transaction/flow-type/@code) > 0)]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Sustainable Development Goals", "/iati-activities/iati-activity[sector[@vocabulary='7' or @vocabulary='8' or @vocabulary='9'] | transaction[sector[@vocabulary='7' or @vocabulary='8' or @vocabulary='9']] | tag[@vocabulary='2' or @vocabulary='3'] | result[indicator[reference[@vocabulary='9']]]]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Identifiers and traceability (participating org role and type)", "/iati-activities/iati-activity[(participating-org/@role='1' or participating-org/@role='2' or participating-org/@role='3' or participating-org/@role='4') and (participating-org/@type='10' or participating-org/@type='11' or participating-org/@type='15' or participating-org/@type='21' or participating-org/@type='22' or participating-org/@type='23' or participating-org/@type='24' or participating-org/@type='30' or participating-org/@type='40' or participating-org/@type='60' or participating-org/@type='70' or participating-org/@type='71' or participating-org/@type='72' or participating-org/@type='73' or participating-org/@type='80' or participating-org/@type='90') and (count(participating-org[@role = '4']) > 0)]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Identifiers and traceability (transaction provider / receiver org type)", "/iati-activities/iati-activity[transaction[transaction-type/@code='2' or transaction-type/@code='3' or transaction-type/@code='5' or transaction-type/@code='6' or transaction-type/@code='7' or transaction-type/@code='8' or transaction-type/@code='10' or transaction-type/@code='12']/receiver-org | transaction[transaction-type/@code='1' or transaction-type/@code='9' or transaction-type/@code='11' or transaction-type/@code='13']/provider-org]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Identifiers and traceability (provider activity id)", "/iati-activities/iati-activity[transaction[transaction-type/@code='1']/provider-org[string-length(@provider-activity-id) > 0]]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Humanitarian (flag)", "/iati-activities/iati-activity[@humanitarian='1' or @humanitarian='true' or transaction/@humanitarian='1' or transaction/@humanitarian='true']", "/iati-activities/iati-activity"),
        ("Data Completeness", "Humanitarian (scope)", "/iati-activities/iati-activity[@humanitarian='1' or @humanitarian='true' or transaction/@humanitarian='1' or transaction/@humanitarian='true'][(string-length(humanitarian-scope/@type) > 0) and (string-length(humanitarian-scope/@code) > 0)]", "/iati-activities/iati-activity[@humanitarian='1' or @humanitarian='true' or transaction/@humanitarian='1' or transaction/@humanitarian='true']"),
        ("Data Completeness", "Humanitarian (cluster)", "/iati-activities/iati-activity[@humanitarian='1' or @humanitarian='true' or transaction/@humanitarian='1' or transaction/@humanitarian='true'][sector[@vocabulary='10'] or transaction/sector[@vocabulary='10']]", "/iati-activities/iati-activity[@humanitarian='1' or @humanitarian='true' or transaction/@humanitarian='1' or transaction/@humanitarian='true']"),
        ("Data Completeness", "Results (indicator)", "/iati-activities/iati-activity[result[indicator]]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Results (baseline)", "/iati-activities/iati-activity[result[indicator[baseline[string-length(@value) > 0]][period[target[string-length(@value) > 0]][actual[string-length(@value) > 0]]]]]", "/iati-activities/iati-activity"),
        ("Data Completeness", "Document Links", "( /iati-activities/iati-activity[document-link[contains(@url, '//') and contains('A01 A02 A03 A04 A05 A06 A07 A08 A09 A10 A11 A12 B01 B02 B03 B04 B05 B06 B07 B08 B09 B10 B11 B12 B13 B14 B15 B16 B17 B18', category/@code)] | result/document-link[contains(@url, '//') and contains('A01 A02 A03 A04 A05 A06 A07 A08 A09 A10 A11 A12 B01 B02 B03 B04 B05 B06 B07 B08 B09 B10 B11 B12 B13 B14 B15 B16 B17 B18', category/@code)]] ) | ( /iati-organisations/iati-organisation[document-link[contains(@url, '//') and contains('A01 A02 A03 A04 A05 A06 A07 A08 A09 A10 A11 A12 B01 B02 B03 B04 B05 B06 B07 B08 B09 B10 B11 B12 B13 B14 B15 B16 B17 B18', category/@code)]] )", "(/iati-activities/iati-activity) | (/iati-organisations/iati-organisation)"),
    ]

    indicator_values = dict()
    indicator_values_aggregate = dict()

    xml_path = os.path.join("/home/alex/git/IATI-Registry-Refresher/data", args.publisher, '*')
    xml_files = glob.glob(xml_path)
    bar = progressbar.ProgressBar()
    for xml_file in bar(xml_files):
        try:
            tree = etree.parse(xml_file, parser=large_parser)
        except etree.XMLSyntaxError:
            continue
        root = tree.getroot()
        
        for category_name, indicator_name, xpath_num, xpath_den in indicators:
            evaluated_value_num = len(root.xpath(xpath_num))
            evaluated_value_den = len(root.xpath(xpath_den))
            if indicator_name + " numerator" not in indicator_values.keys():
                indicator_values[indicator_name + " numerator"] = evaluated_value_num
            else:
                indicator_values[indicator_name + " numerator"] += evaluated_value_num
            if indicator_name + " denominator" not in indicator_values.keys():
                indicator_values[indicator_name + " denominator"] = evaluated_value_den
            else:
                indicator_values[indicator_name + " denominator"] += evaluated_value_den
        destroy_tree(tree)

    if len(indicator_values.keys()) > 0:
        for category_name, indicator_name, xpath_num, xpath_den in indicators:
            try:
                evaluated_value = indicator_values[indicator_name + " numerator"] / indicator_values[indicator_name + " denominator"]
            except ZeroDivisionError:
                evaluated_value = "NA"
            indicator_values_aggregate[indicator_name] = evaluated_value
    outfile = os.path.join(output_dir, "{}.json".format(args.publisher))
    with open(outfile, "w") as json_file:
        json_file.write(json.dumps(indicator_values_aggregate, indent=4))
        
