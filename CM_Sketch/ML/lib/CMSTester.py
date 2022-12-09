import pandas as pd
from .CountMinSketch import CountMinSketch
from math import ceil
from concurrent.futures import ProcessPoolExecutor, as_completed

# Globals
depths = [x for x in range(2, 100)]
widths = [percentage/100 for percentage in range(1, 101)]
top_k = [percentile/100 for percentile in range(1, 101)]
PROCESSOR_COUNT = 16


def read_data_from_file(filePath):
    data = None
    try:
        data = pd.read_pickle(filePath)
    except:
        data = pd.read_csv(filePath)
    return data


def prepareFlowsFromDataset(datasetPath, outputPath):
    ''' Function to read dataset, parse through packets and prepare flow count
    '''
    # Read dataset
    data = read_data_from_file(datasetPath)
    print(f"{datasetPath} read successfully!")

    # Iterate through dataset and find unique flows and their packet count
    flow_packet_count = {}

    for index, row in data.iterrows():
        flow_key = row['src'] + "|" + row['dst'] + "|" + str(int(row['sport'])) + "|" + str(int(row['dport'])) + "|" + str(int(row['proto']))
        if flow_key not in flow_packet_count:
            flow_packet_count[flow_key] = 1
        else:
            flow_packet_count[flow_key] += 1
    unique_flows = len(flow_packet_count)
    print(f"Flow calculations done. Found {unique_flows} flows!")

    df = pd.DataFrame(flow_packet_count.items(), columns=['Flow ID', 'Packet Count'])
    df.to_pickle(f'{outputPath}/flow_packet_count.pkl')

    return f'{outputPath}/flow_packet_count.pkl'


def parameterExploration(datasetPath, outputPath):
    ''' Function to loop through depth and width combinations for hyper-parameter tuning
    '''
    # Read dataset
    data = read_data_from_file(datasetPath)
    print(f"{datasetPath} read successfully!")

    unique_flows = len(data)

    for depth in depths:
        results = []
        with ProcessPoolExecutor(max_workers=PROCESSOR_COUNT) as executor:
            futures = {executor.submit(getCMAccuracy, data, ceil(width_perc * unique_flows), depth): width_perc for width_perc in widths}
            for future in as_completed(futures):
                partial = future.result()
                results += partial

        # Writing result to disk as a pkl
        results_df = pd.DataFrame.from_dict(results)
        results_df.to_pickle(f'{outputPath}/results_{depth}.pkl')
        print(f'Wrote DF to {outputPath}/results_{depth}.pkl')


def getCMAccuracy(flow_packet_count, width, depth):
    # Finding reverse sorted list of flows for finding top k heavy hitters
    actual_heavy_hitters = flow_packet_count.sort_values(by='Packet Count', ascending=False)

    result = []
    cm = CountMinSketch(width, depth)

    # Simulate CM Sketch counting
    for index, row in flow_packet_count.iterrows():
        flow = row['Flow ID']
        flow_count = row['Packet Count']
        for i in range(0, flow_count):
            cm.increment(flow)

    # Get estimate of the heavy hitters from CM Sketch
    cms_heavy_hitters = []
    for index, row in flow_packet_count.iterrows():
        flow = row['Flow ID']
        cms_heavy_hitters.append([flow, cm.estimate(flow)])

    # Constructing DataFrame
    cms_heavy_hitters = pd.DataFrame(cms_heavy_hitters, columns=['Flow ID', 'Packet Count']).sort_values(by='Packet Count', ascending=False)
    cms_heavy_hitters = cms_heavy_hitters.convert_dtypes()

    # Calculate accuracy of top k hitters, as a varying percentile
    for percentile in top_k:
        actual_quantile = actual_heavy_hitters.quantile(percentile)[0]
        cms_quantile = cms_heavy_hitters.quantile(percentile)[0]

        heavy_actual_flows = set(actual_heavy_hitters.loc[actual_heavy_hitters['Packet Count'] >= actual_quantile]['Flow ID'])
        heavy_cms_flows = set(cms_heavy_hitters.loc[cms_heavy_hitters['Packet Count'] >= cms_quantile]['Flow ID'])

        flow_matches = len(heavy_actual_flows.intersection(heavy_cms_flows))
        accuracy = flow_matches / (len(heavy_actual_flows) + len(heavy_cms_flows) - flow_matches)

        # Appending to result array for capturing
        result.append({
            'cm_depth': depth,
            'cm_width': width,
            'k_percentile': percentile,
            'accuracy': accuracy
        })
        print(f"cm_depth: {depth}\tcm_width: {width}\tk_percentile: {percentile}\taccuracy: {accuracy}")

    return result
