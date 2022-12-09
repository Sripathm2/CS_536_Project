from lib.CMSTester import prepareFlowsFromDataset, parameterExploration

if __name__ == '__main__':
    flow_dataset_path = prepareFlowsFromDataset(datasetPath='./dataset_0.pkl', outputPath='./results')
    parameterExploration(datasetPath=flow_dataset_path, outputPath='./results')
